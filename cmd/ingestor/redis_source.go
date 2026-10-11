package main

// Redis live source: read pending (not-yet-committed) packet observations
// from a ukmesh RAM-first ingest stream and feed them through the same
// handleMessage pipeline as MQTT traffic.
//
// The upstream RAM-first ingest admits validated packets to a bounded Redis
// stream and acknowledges the broker at admission; a background writer
// persists batches to PostgreSQL moments later and removes each entry as it
// commits (commit markers are written as an explicit boundary). This source
// is the live FAST-PATH on top of that: it makes new observations visible to
// CoreScope without waiting for the database flush window.
//
// Correctness notes:
//   - entries are read with plain XRANGE (no consumer group): readers must
//     never consume entries away from the upstream writer/other readers, and
//     re-reading is harmless;
//   - the postgresSources follow continues to deliver the same observations
//     from the database later; CoreScope's observation upsert
//     (transmission_id, observer, path) collapses the live.copy into the
//     same row, and the later database copy wins with the fuller payload;
//   - nothing is lost when this source is down: the database follow remains
//     the completeness path. The cursor only skips live delivery, never data;
//   - privacy: the upstream entries carry admission-time visibility flags;
//     with requirePublicVisibility (default true) only visibility_ok entries
//     are fed — the same guard the postgres source applies to stored rows.

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
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
	if err := json.Unmarshal(raw, &st); err != nil || st.LastID == "" {
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
	time         string
	topic        string
	network      string
	rawHex       string
	packetHash   string
	snr          *float64
	rssi         *float64
	direction    string
	visibilityOK bool
}

// decodeRedisStreamValue parses one stream value in the upstream v1 wire
// format: {"v":1,"k":"p","o":…,"a":…,"ip":0|1,"vo":0|1,"d":{…input…}}.
// Commit markers (k:"c") and malformed values return ok=false; callers skip
// them and advance the cursor past them.
func decodeRedisStreamValue(value string) (redisPendingEntry, bool) {
	var v struct {
		V  int             `json:"v"`
		K  string          `json:"k"`
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
		Time        string   `json:"time"`
		PacketHash  string   `json:"packetHash"`
		Topic       string   `json:"topic"`
		Network     string   `json:"network"`
		RawHex      string   `json:"rawHex"`
		SNR         *float64 `json:"snr"`
		RSSI        *float64 `json:"rssi"`
		PayloadJSON *string  `json:"payloadJson"`
	}
	if err := json.Unmarshal(v.D, &d); err != nil {
		return redisPendingEntry{}, false
	}
	if d.Time == "" || d.RawHex == "" || d.Topic == "" {
		return redisPendingEntry{}, false
	}
	entry := redisPendingEntry{
		time:         d.Time,
		topic:        d.Topic,
		network:      d.Network,
		rawHex:       d.RawHex,
		packetHash:   d.PacketHash,
		snr:          d.SNR,
		rssi:         d.RSSI,
		visibilityOK: v.VO == 1,
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
	if src.PublicVisibilityRequired() && !e.visibilityOK {
		return false
	}
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
	tag := src.Name
	if tag == "" {
		tag = "redis"
	}
	if src.URL == "" {
		log.Printf("[redis:%s] ERROR: no url configured — source disabled", tag)
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

		type liveRow struct {
			msg *pgMessage
		}
		rows := make([]liveRow, 0, len(entries))
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
			if !redisEntryAllowed(entry, src) {
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
				continue
			}
			rows = append(rows, liveRow{msg: &pgMessage{topic: entry.topic, payload: payload}})
		}

		for ingestBuffer.Pending() > bufferHighWater {
			time.Sleep(25 * time.Millisecond)
		}

		droppedBefore := ingestBuffer.Dropped()
		batchStart := time.Now()
		for i := range rows {
			r := rows[i]
			ingestBuffer.Submit(func() {
				handleMessage(store, tag, srcForHandler, r.msg, keySet.Snapshot(), regionSet, cfg)
			})
		}

		// Wait for the batch to fully drain before advancing the cursor: it
		// must never point past entries that have not reached the database.
		for ingestBuffer.Pending() > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		if d := ingestBuffer.Dropped() - droppedBefore; d > 0 {
			log.Printf("[redis:%s] WARNING: ingest buffer dropped %d entr(y/ies) — not advancing cursor, retrying batch", tag, d)
			time.Sleep(time.Second)
			continue
		}

		state.LastID = entries[len(entries)-1].ID
		if err := saveRedisSourceState(statePath, state); err != nil {
			log.Printf("[redis:%s] cursor save error (non-fatal; cursor held in memory): %v", tag, err)
		}
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
