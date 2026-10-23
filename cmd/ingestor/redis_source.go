package main

// Redis live source: read pending (not-yet-committed) packet observations
// from a ukmesh RAM-first ingest stream and feed them through the same
// handleMessage pipeline as MQTT traffic.
//
// The upstream RAM-first ingest admits validated packets to a bounded Redis
// stream and acknowledges the broker at admission; a background writer
// persists batches to PostgreSQL moments later and retains committed entries
// for a bounded reader overlap. This source
// is the live FAST-PATH on top of that: it makes new observations visible to
// CoreScope without waiting for the database flush window.
//
// Correctness notes:
//   - entries are read with plain XRANGE (no consumer group): readers must
//     never consume entries away from the upstream writer/other readers, and
//     re-reading is harmless;
//   - the paired postgresSources follower uses a committed-order ledger after
//     Redis retention; stable upstream UUIDs collapse duplicate delivery;
//   - public batches load current UKMesh policy. Admission-time visibility
//     flags are metadata, never current authorization.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisZeroID sorts before every real stream ID; used as the initial cursor
// when no state exists and the stream is empty.
const redisZeroID = "0-0"

// redisSourceState is the persisted cursor: the ID of the last stream entry
// whose preceding batch fully drained into the database.
type redisSourceState struct {
	LastID string `json:"lastId"`
}

// loadRedisSourceState reads the cursor file. A missing or corrupt file reads
// as "no cursor" — the caller starts from the stream tail.
func loadRedisSourceState(path string) (redisSourceState, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return redisSourceState{}, false
	}
	var st redisSourceState
	if err := json.Unmarshal(raw, &st); err != nil || !validRedisID(st.LastID) {
		return redisSourceState{}, false
	}
	return st, true
}

// saveRedisSourceState writes the cursor atomically (tmp + rename) so a crash
// mid-write can never leave a truncated state file.
func saveRedisSourceState(path string, st redisSourceState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// defaultRedisStatePath is the cursor location when the source config does not
// set one: next to the analyzer config/database, one file per source.
func defaultRedisStatePath(configDir, name string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, name)
	if safe == "" {
		safe = "default"
	}
	return filepath.Join(configDir, "redis-source-"+safe+".state.json")
}

// redisPendingEntry is the decoded subset of an upstream stream entry this
// source needs to build an ingest envelope.
type redisPendingEntry struct {
	observationID     string
	time              string
	topic             string
	network           string
	rawHex            string
	packetHash        string
	rxNodeID          string
	srcNodeID         string
	pathHashes        []string
	pathHashSizeBytes int
	snr               *float64
	rssi              *float64
	direction         string
	visibilityOK      bool
}

// decodeRedisStreamValue parses one stream value in the upstream v1 wire
// format: {"v":1,"k":"p","o":…,"a":…,"ip":0|1,"vo":0|1,"d":{…input…}}.
// Commit markers (k:"c") and malformed values return ok=false; callers skip
// them and advance the cursor past them.
func decodeRedisStreamValue(value string) (redisPendingEntry, bool) {
	var v struct {
		V  int             `json:"v"`
		K  string          `json:"k"`
		O  string          `json:"o"`
		VO int             `json:"vo"`
		D  json.RawMessage `json:"d"`
	}
	if err := json.Unmarshal([]byte(value), &v); err != nil {
		return redisPendingEntry{}, false
	}
	if v.V != 1 || v.K != "p" || len(v.D) == 0 {
		return redisPendingEntry{}, false
	}
	var d struct {
		Time              string   `json:"time"`
		PacketHash        string   `json:"packetHash"`
		Topic             string   `json:"topic"`
		Network           string   `json:"network"`
		RawHex            string   `json:"rawHex"`
		RXNodeID          string   `json:"rxNodeId"`
		SrcNodeID         string   `json:"srcNodeId"`
		PathHashes        []string `json:"pathHashes"`
		PathHashSizeBytes *int     `json:"pathHashSizeBytes"`
		SNR               *float64 `json:"snr"`
		RSSI              *float64 `json:"rssi"`
		PayloadJSON       *string  `json:"payloadJson"`
	}
	if err := json.Unmarshal(v.D, &d); err != nil {
		return redisPendingEntry{}, false
	}
	if d.Time == "" || d.RawHex == "" || d.Topic == "" {
		return redisPendingEntry{}, false
	}
	entry := redisPendingEntry{
		observationID: v.O,
		time:          d.Time,
		topic:         d.Topic,
		network:       d.Network,
		rawHex:        d.RawHex,
		packetHash:    d.PacketHash,
		rxNodeID:      d.RXNodeID,
		srcNodeID:     d.SrcNodeID,
		pathHashes:    d.PathHashes,
		snr:           d.SNR,
		rssi:          d.RSSI,
		visibilityOK:  v.VO == 1,
	}
	if d.PathHashSizeBytes != nil {
		entry.pathHashSizeBytes = *d.PathHashSizeBytes
	}
	if d.PayloadJSON != nil && *d.PayloadJSON != "" {
		var pj struct {
			Direction string `json:"direction"`
		}
		if err := json.Unmarshal([]byte(*d.PayloadJSON), &pj); err == nil {
			entry.direction = pj.Direction
		}
	}
	return entry, true
}

// redisEntryAllowed applies the source's scope filters to a decoded entry.
func redisEntryAllowed(e redisPendingEntry, src RedisSource) bool {
	if len(src.Networks) > 0 {
		for _, n := range src.Networks {
			if n == e.network {
				return true
			}
		}
		return false
	}
	return true
}

type corePrivatePrefix struct {
	NodeID  string `json:"node_id"`
	Network string `json:"network"`
	Size    int    `json:"prefix_size_bytes"`
	Prefix  string `json:"prefix"`
}

type corePublicPolicy struct {
	generation  int64
	prefixes    []corePrivatePrefix
	fingerprint string // digest of prefixes only — excludes the generation counter
	nodes       map[string]struct{}
	paths       map[string]struct{}
}

func loadCorePublicPolicy(ctx context.Context, db *sql.DB) (*corePublicPolicy, error) {
	var generation, materialized int64
	var active bool
	var raw []byte
	err := db.QueryRowContext(ctx, `
		SELECT visibility.generation, materialized.visibility_generation,
		       EXISTS (SELECT 1 FROM privacy_rematerialization_queue
		               WHERE status IN ('pending', 'processing', 'failed')),
		       COALESCE(jsonb_agg(jsonb_build_object(
		         'node_id', prefix.node_id, 'network', prefix.network,
		         'prefix_size_bytes', prefix.prefix_size_bytes, 'prefix', prefix.prefix
		       )) FILTER (WHERE prefix.node_id IS NOT NULL), '[]'::jsonb)
		FROM public_visibility_state visibility
		JOIN packet_visibility_materialization_state materialized
		  ON materialized.singleton = visibility.singleton
		LEFT JOIN private_node_prefixes prefix ON TRUE
		WHERE visibility.singleton = TRUE
		GROUP BY visibility.generation, materialized.visibility_generation`).Scan(
		&generation, &materialized, &active, &raw)
	if err != nil {
		return nil, err
	}
	if generation < 1 || materialized != generation || active {
		return nil, errors.New("UKMesh public privacy authority is stale or rematerializing")
	}
	var prefixes []corePrivatePrefix
	if err := json.Unmarshal(raw, &prefixes); err != nil {
		return nil, err
	}
	return &corePublicPolicy{generation: generation, prefixes: prefixes, fingerprint: corePolicyFingerprint(prefixes)}, nil
}

// corePolicyFingerprint digests the privacy rules that decide visibility.
// The generation counter is deliberately excluded: it moves on every trigger
// bump — including FK-cascade node deletes and zero-row statements that
// change no rule — and a projection rebuild is only required when the rules
// themselves differ. Normalisation mirrors index() so two orderings of the
// same rules always hash the same.
func corePolicyFingerprint(prefixes []corePrivatePrefix) string {
	parts := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		parts = append(parts, fmt.Sprintf("%s|%s|%d|%s", coreNetworkKey(prefix.Network),
			strings.ToUpper(prefix.NodeID), prefix.Size, strings.ToUpper(prefix.Prefix)))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func coreUKFamily(network string) bool {
	return network == "ukmesh" || network == "northeast" || network == "teesside"
}
func coreNetworkKey(network string) string {
	if coreUKFamily(network) {
		return "ukmesh-family"
	}
	return network
}

func (p *corePublicPolicy) index() {
	if p.nodes != nil {
		return
	}
	p.nodes = make(map[string]struct{}, len(p.prefixes))
	p.paths = make(map[string]struct{}, len(p.prefixes))
	for _, prefix := range p.prefixes {
		network := coreNetworkKey(prefix.Network)
		p.nodes[network+":"+strings.ToUpper(prefix.NodeID)] = struct{}{}
		p.paths[fmt.Sprintf("%s:%d:%s", network, prefix.Size, strings.ToUpper(prefix.Prefix))] = struct{}{}
	}
}

func (p *corePublicPolicy) allows(e redisPendingEntry) bool {
	if p == nil {
		return false
	}
	p.index()
	if len(e.pathHashes) > 0 {
		if e.pathHashSizeBytes < 1 || e.pathHashSizeBytes > 3 {
			return false
		}
		for _, path := range e.pathHashes {
			if len(path) != e.pathHashSizeBytes*2 {
				return false
			}
			for _, c := range path {
				if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')) {
					return false
				}
			}
		}
	}
	network := coreNetworkKey(e.network)
	if _, private := p.nodes[network+":"+strings.ToUpper(e.rxNodeID)]; private {
		return false
	}
	if _, private := p.nodes[network+":"+strings.ToUpper(e.srcNodeID)]; private {
		return false
	}
	for _, path := range e.pathHashes {
		if _, private := p.paths[fmt.Sprintf("%s:%d:%s", network, e.pathHashSizeBytes, strings.ToUpper(path))]; private {
			return false
		}
	}
	return true
}

// redisEnvelopeTimestamp normalises the entry timestamp for the ingest
// envelope, preserving sub-second precision when parseable.
func redisEnvelopeTimestamp(raw string) string {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// runRedisSource drives one configured RedisSource. Intended to run in its own
// goroutine for the process lifetime; the cursor is persisted after every
// drained batch so a restart resumes without re-delivering.
func runRedisSource(store *Store, src RedisSource, keySet *channelKeySet, regionSet *regionKeySet, cfg *Config, ingestBuffer *IngestBuffer, configDir string) {
	if src.PublicVisibilityRequired() {
		runPublicLiveSource(store, src, ingestBuffer, configDir)
		return
	}
	tag := src.Name
	if tag == "" {
		tag = "redis"
	}
	if src.URL == "" {
		log.Printf("[redis:%s] ERROR: no url configured — source disabled", tag)
		return
	}
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		log.Printf("[redis:%s] ERROR: %v", tag, err)
		return
	}

	opts, err := redis.ParseURL(src.URL)
	if err != nil {
		log.Printf("[redis:%s] ERROR: unparseable url: %v", tag, err)
		return
	}
	if src.Password != "" {
		opts.Password = src.Password
	}
	client := redis.NewClient(opts)
	defer client.Close()
	if len(cfg.PostgresSources) != 1 {
		log.Printf("[redis:%s] ERROR: exactly one paired PostgreSQL completeness source is required", tag)
		return
	}
	completenessDB, err := sql.Open("postgres", cfg.PostgresSources[0].DSN)
	if err != nil {
		log.Printf("[redis:%s] ERROR: completeness database: %v", tag, err)
		return
	}
	defer completenessDB.Close()
	var hasLedger bool
	if err := completenessDB.QueryRow(`SELECT to_regclass('ingest_observation_ledger') IS NOT NULL`).Scan(&hasLedger); err != nil || !hasLedger {
		log.Printf("[redis:%s] ERROR: committed-order ledger unavailable; source disabled: %v", tag, err)
		return
	}
	var privacyDB *sql.DB
	if src.PublicVisibilityRequired() {
		privacyDSN := src.PrivacyDSN
		if privacyDSN == "" {
			privacyDSN = cfg.PostgresSources[0].DSN
		}
		privacyDB, err = sql.Open("postgres", privacyDSN)
		if err != nil {
			log.Printf("[redis:%s] ERROR: privacy database: %v", tag, err)
			return
		}
		defer privacyDB.Close()
	}
	streamKey := src.StreamKeyOrDefault()

	// Initial connectivity probe with a bounded retry loop — redis may still
	// be starting when the ingestor boots.
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = client.Ping(ctx).Err()
		cancel()
		if err == nil {
			break
		}
		if attempt >= 12 {
			log.Printf("[redis:%s] ERROR: redis unreachable after %d attempts: %v — source will keep retrying in the poll loop", tag, attempt, err)
			break
		}
		log.Printf("[redis:%s] waiting for redis (%v) — attempt %d", tag, err, attempt)
		time.Sleep(5 * time.Second)
	}
	log.Printf("[redis:%s] connected (stream %s)", tag, streamKey)

	statePath := src.StatePath
	if statePath == "" {
		statePath = defaultRedisStatePath(configDir, tag)
	}
	state, resumed := loadRedisSourceState(statePath)
	if resumed {
		log.Printf("[redis:%s] resuming from cursor %s", tag, state.LastID)
	} else {
		// Live fast-path: start from the stream tail, never replay history —
		// the postgres source owns backfill/completeness.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		tail, xerr := client.XRevRangeN(ctx, streamKey, "+", "-", 1).Result()
		cancel()
		if xerr == nil && len(tail) > 0 {
			state = redisSourceState{LastID: tail[0].ID}
			log.Printf("[redis:%s] following new entries only (tail %s)", tag, state.LastID)
		} else {
			state = redisSourceState{LastID: redisZeroID}
			log.Printf("[redis:%s] following new entries only (empty stream)", tag)
		}
	}

	// Register with the status registry + liveness watchdog so health
	// surfaces treat this source like any other. There is no MQTT client to
	// force-drop on a stall: IsConnectedFn pings redis and ForceReconnectFn
	// stays nil (the watchdog treats nil as a no-op).
	status := RegisterSourceStatus(tag, "redis")
	liveness := &SourceLivenessState{
		Tag:    tag,
		Broker: "redis:" + streamKey,
	}
	liveness.IsConnectedFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return client.Ping(ctx).Err() == nil
	}
	registerLivenessOrSkip(liveness)

	srcForHandler := MQTTSource{
		Name:                 tag,
		PreserveEnvelopeTime: true,
		MaxEnvelopePastDays:  src.MaxEnvelopePastDays,
	}

	batchSize := src.BatchSizeOrDefault()
	pollInterval := time.Duration(src.PollIntervalMsOrDefault()) * time.Millisecond

	// Never let the bounded ingest buffer fill: Submit() drops when full, so
	// wait for the writer to drain below the high-water mark first (same
	// discipline as the postgres source).
	const bufferHighWater = 2000

	connected := false
	backoff := time.Second

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		entries, xerr := client.XRangeN(ctx, streamKey, "("+state.LastID, "+", int64(batchSize)).Result()
		cancel()
		if xerr != nil {
			status.MarkDisconnect(time.Now(), xerr)
			log.Printf("[redis:%s] read error: %v — retry in %s", tag, xerr, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		if !connected {
			status.MarkConnect(time.Now())
			connected = true
		}
		backoff = time.Second
		markReceiptForTag(tag, time.Now())

		if len(entries) == 0 {
			status.MarkPacket(time.Now())
			time.Sleep(pollInterval)
			continue
		}
		var policy *corePublicPolicy
		if privacyDB != nil {
			privacyCtx, cancelPrivacy := context.WithTimeout(context.Background(), 10*time.Second)
			policy, err = loadCorePublicPolicy(privacyCtx, privacyDB)
			cancelPrivacy()
			if err != nil {
				log.Printf("[redis:%s] public authority unavailable; cursor held: %v", tag, err)
				time.Sleep(time.Second)
				continue
			}
		}

		type liveRow struct {
			msg *pgMessage
		}
		rows := make([]liveRow, 0, len(entries))
		decodeFailed := false
		for _, x := range entries {
			var raw string
			switch v := x.Values["e"].(type) {
			case string:
				raw = v
			case []byte:
				raw = string(v)
			}
			entry, ok := decodeRedisStreamValue(raw)
			if !ok {
				continue // commit marker or malformed: skip, cursor advances below
			}
			if !redisEntryAllowed(entry, src) || (src.PublicVisibilityRequired() && !policy.allows(entry)) {
				continue
			}
			env := pgEnvelope{
				Raw:       entry.rawHex,
				Timestamp: redisEnvelopeTimestamp(entry.time),
				SNR:       entry.snr,
				RSSI:      entry.rssi,
				Direction: entry.direction,
				Origin:    "",
			}
			payload, merr := json.Marshal(env)
			if merr != nil {
				decodeFailed = true
				log.Printf("[redis:%s] envelope encoding failed; cursor held: %v", tag, merr)
				continue
			}
			rows = append(rows, liveRow{msg: &pgMessage{topic: entry.topic, payload: payload, obsID: entry.observationID}})
		}

		for ingestBuffer.Pending() > bufferHighWater {
			time.Sleep(25 * time.Millisecond)
		}

		droppedBefore := ingestBuffer.Dropped()
		batchStart := time.Now()
		completed := make([]<-chan error, 0, len(rows))
		admissionFailed := false
		for i := range rows {
			r := rows[i]
			result, accepted := ingestBuffer.SubmitTracked(func() error {
				return handleMessage(store, tag, srcForHandler, r.msg, keySet.Snapshot(), regionSet, cfg)
			})
			if !accepted {
				admissionFailed = true
				continue
			}
			completed = append(completed, result)
		}

		nextState := redisSourceState{LastID: entries[len(entries)-1].ID}
		if err := finishSourceBatch(completed, admissionFailed || decodeFailed || ingestBuffer.Dropped() > droppedBefore,
			func() error { return saveRedisSourceState(statePath, nextState) }); err != nil {
			log.Printf("[redis:%s] batch incomplete (cursor held for retry): %v", tag, err)
			time.Sleep(time.Second)
			continue
		}
		state = nextState
		status.MarkPacket(time.Now())

		elapsed := time.Since(batchStart)
		if src.MaxRowsPerSec > 0 {
			minDur := time.Duration(float64(len(entries)) / float64(src.MaxRowsPerSec) * float64(time.Second))
			if elapsed < minDur {
				time.Sleep(minDur - elapsed)
			}
		}
		if len(entries) < batchSize {
			time.Sleep(pollInterval)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// redisRedactURL strips credentials from a URL for logs (mirrors redactPgDSN).
func redisRedactURL(raw string) string {
	if i := strings.Index(raw, "://"); i >= 0 {
		rest := raw[i+3:]
		if at := strings.Index(rest, "@"); at >= 0 {
			cred := rest[:at]
			if c := strings.Index(cred, ":"); c >= 0 {
				cred = cred[:c] + ":***"
			} else {
				cred = "***"
			}
			return raw[:i+3] + cred + rest[at:]
		}
	}
	return raw
}
