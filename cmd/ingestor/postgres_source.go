package main

// Postgres ingest source: backfill + follow from an existing meshcore
// PostgreSQL/TimescaleDB deployment.
//
// The source reads one row per observer reception from the `packets`
// hypertable (time, topic, raw_hex, snr, rssi, payload->>'direction',
// visibility_ok, is_private, network, observation_id) and feeds each row
// through the same handleMessage pipeline as live MQTT traffic — decoding,
// region scoping, node/observer upserts and observation dedup all behave
// exactly as if the packets had just arrived over MQTT.
//
// Two modes, selected by cursor state:
//   - Backfill: no cursor yet and startTime set — read forward from
//     startTime in (time, observation_id) order, persisting the cursor
//     after every fully-drained batch (crash-safe resume).
//   - Follow: cursor exists (or startTime empty) — poll for new rows.
//
// Historical fidelity: the synthetic source sets PreserveEnvelopeTime so
// packet/observation times come from the stored rx time instead of the
// replay wall clock (see #1370 for why live sources do the opposite).
//
// Backpressure: IngestBuffer.Submit drops when full (QoS-0 semantics, fine
// for live MQTT loss) — for a backfill a dropped row is silent history
// loss, so this source paces on buffer occupancy and never advances its
// cursor past rows that have not drained into the database.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
)

// pgZeroObsID is the UUID substituted for rows with a NULL observation_id in
// keyset pagination — it sorts before every real UUID and makes the
// (time, observation_id) tuple comparison total.
const pgZeroObsID = "00000000-0000-0000-0000-000000000000"

// pgSourceState is the persisted cursor: the (time, observation_id) pair of
// the last row whose batch fully drained into the database. Resuming from
// this pair re-reads nothing and skips nothing.
type pgSourceState struct {
	LastTime  string `json:"lastTime"`  // RFC3339Nano, UTC
	LastObsID string `json:"lastObsId"` // UUID as text
}

// loadPgSourceState reads the cursor file. A missing or corrupt file reads
// as "no cursor" — the caller falls back to startTime/now.
func loadPgSourceState(path string) (pgSourceState, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return pgSourceState{}, false
	}
	var st pgSourceState
	if err := json.Unmarshal(raw, &st); err != nil || st.LastTime == "" {
		return pgSourceState{}, false
	}
	return st, true
}

// savePgSourceState writes the cursor atomically (tmp + rename) so a crash
// mid-write can never leave a truncated state file.
func savePgSourceState(path string, st pgSourceState) error {
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

// pgMessage adapts a database row to the mqtt.Message interface so it can be
// submitted through the same ingest path as broker messages.
type pgMessage struct {
	topic   string
	payload []byte
}

func (m *pgMessage) Topic() string     { return m.topic }
func (m *pgMessage) Payload() []byte   { return m.payload }
func (m *pgMessage) Qos() byte         { return 0 }
func (m *pgMessage) Retained() bool    { return false }
func (m *pgMessage) Duplicate() bool   { return false }
func (m *pgMessage) MessageID() uint16 { return 0 }
func (m *pgMessage) Ack()              {}

// pgEnvelope is the JSON payload the handler parses; it mirrors what
// observers publish to MQTT.
type pgEnvelope struct {
	Raw       string   `json:"raw"`
	Timestamp string   `json:"timestamp,omitempty"`
	SNR       *float64 `json:"SNR,omitempty"`
	RSSI      *float64 `json:"RSSI,omitempty"`
	Direction string   `json:"direction,omitempty"`
	Origin    string   `json:"origin,omitempty"`
}

// pgRow is one scanned packets row.
type pgRow struct {
	Time       time.Time
	Topic      string
	RawHex     string
	SNR        *float64
	RSSI       *float64
	Direction  string
	OriginName string
	ObsID      string
}

// buildPgBatchQuery returns the keyset-paginated SELECT and its args.
// $1/$2 are the cursor (time, observation_id). Optional filters: a network
// allow-list and the public-visibility guard. Kept as a pure function so
// the filter shapes are unit-testable without a live database.
func buildPgBatchQuery(src PostgresSource, cursor pgSourceState, batchSize int) (string, []interface{}) {
	var b strings.Builder
	args := []interface{}{cursor.LastTime, cursor.LastObsID}
	b.WriteString(`SELECT p.time, p.topic, p.raw_hex, p.snr, p.rssi,
       COALESCE(p.payload->>'direction', '') AS direction,
       COALESCE(n.name, '') AS origin_name,
       COALESCE(p.observation_id, '` + pgZeroObsID + `'::uuid)::text AS obs_id
FROM packets p
LEFT JOIN nodes n ON n.public_key = p.rx_node_id
WHERE (p.time, COALESCE(p.observation_id, '` + pgZeroObsID + `'::uuid)) > ($1::timestamptz, $2::uuid)
  AND p.raw_hex IS NOT NULL AND p.raw_hex <> ''`)
	if len(src.Networks) > 0 {
		args = append(args, pq.Array(src.Networks))
		b.WriteString(fmt.Sprintf("\n  AND p.network = ANY($%d)", len(args)))
	}
	if src.PublicVisibilityRequired() {
		b.WriteString("\n  AND p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE")
	}
	b.WriteString(fmt.Sprintf("\nORDER BY p.time, COALESCE(p.observation_id, '%s'::uuid)\nLIMIT $%d", pgZeroObsID, len(args)+1))
	args = append(args, batchSize)
	return b.String(), args
}

// defaultPgStatePath is the cursor location when the source config does not
// set one: next to the analyzer config/database, one file per source.
func defaultPgStatePath(configDir, name string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, name)
	if safe == "" {
		safe = "default"
	}
	return filepath.Join(configDir, "pg-source-"+safe+".state.json")
}

// redactPgDSN strips credentials from a DSN for logs.
func redactPgDSN(dsn string) string {
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if at := strings.Index(rest, "@"); at >= 0 {
			cred := rest[:at]
			if c := strings.Index(cred, ":"); c >= 0 {
				cred = cred[:c] + ":***"
			} else {
				cred = "***"
			}
			return dsn[:i+3] + cred + rest[at:]
		}
	}
	return dsn
}

// scanPgRows drains a result set into pgRows.
func scanPgRows(rows *sql.Rows) ([]pgRow, error) {
	out := make([]pgRow, 0, 64)
	for rows.Next() {
		var (
			r         pgRow
			snr, rssi sql.NullFloat64
		)
		if err := rows.Scan(&r.Time, &r.Topic, &r.RawHex, &snr, &rssi, &r.Direction, &r.OriginName, &r.ObsID); err != nil {
			return nil, err
		}
		if snr.Valid {
			v := snr.Float64
			r.SNR = &v
		}
		if rssi.Valid {
			v := rssi.Float64
			r.RSSI = &v
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// runPostgresSource drives one configured PostgresSource. Intended to run in
// its own goroutine for the process lifetime; the cursor is persisted after
// every drained batch so a SIGTERM mid-backfill resumes cleanly.
func runPostgresSource(store *Store, src PostgresSource, keySet *channelKeySet, regionSet *regionKeySet, cfg *Config, ingestBuffer *IngestBuffer, configDir string) {
	tag := src.Name
	if tag == "" {
		tag = "pg"
	}
	if src.DSN == "" {
		log.Printf("[pg:%s] ERROR: no dsn configured — source disabled", tag)
		return
	}

	db, err := sql.Open("postgres", src.DSN)
	if err != nil {
		log.Printf("[pg:%s] ERROR: open: %v", tag, err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Initial connectivity probe with a bounded retry loop — the database
	// may still be starting when the ingestor boots.
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if attempt >= 12 {
			log.Printf("[pg:%s] ERROR: database unreachable after %d attempts: %v — source will keep retrying in the poll loop", tag, attempt, err)
			break
		}
		log.Printf("[pg:%s] waiting for database (%v) — attempt %d", tag, err, attempt)
		time.Sleep(5 * time.Second)
	}
	log.Printf("[pg:%s] connected to %s", tag, redactPgDSN(src.DSN))

	statePath := src.StatePath
	if statePath == "" {
		statePath = defaultPgStatePath(configDir, tag)
	}

	state, resumed := loadPgSourceState(statePath)
	switch {
	case resumed:
		log.Printf("[pg:%s] resuming from cursor time=%s obs=%s", tag, state.LastTime, state.LastObsID)
	case src.StartTime != "":
		t, perr := time.Parse(time.RFC3339, src.StartTime)
		if perr != nil {
			log.Printf("[pg:%s] ERROR: unparseable startTime %q: %v — starting from now instead", tag, src.StartTime, perr)
			state = pgSourceState{LastTime: time.Now().UTC().Format(time.RFC3339Nano), LastObsID: pgZeroObsID}
		} else {
			state = pgSourceState{LastTime: t.UTC().Format(time.RFC3339Nano), LastObsID: pgZeroObsID}
			log.Printf("[pg:%s] backfill from %s", tag, t.UTC().Format(time.RFC3339))
		}
	default:
		state = pgSourceState{LastTime: time.Now().UTC().Format(time.RFC3339Nano), LastObsID: pgZeroObsID}
		log.Printf("[pg:%s] no startTime — following new rows only", tag)
	}

	// Register with the status registry + liveness watchdog so health
	// surfaces treat this source like any other. There is no MQTT client
	// to force-drop on a stall: IsConnectedFn pings the database and
	// ForceReconnectFn stays nil (the watchdog treats nil as a no-op).
	status := RegisterSourceStatus(tag, "postgres")
	liveness := &SourceLivenessState{
		Tag:    tag,
		Broker: "postgres",
	}
	liveness.IsConnectedFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil
	}
	registerLivenessOrSkip(liveness)

	srcForHandler := MQTTSource{
		Name:                 tag,
		PreserveEnvelopeTime: true,
		MaxEnvelopePastDays:  src.MaxEnvelopePastDays,
	}

	batchSize := src.BatchSizeOrDefault()
	pollInterval := time.Duration(src.PollIntervalOrDefault()) * time.Second

	// Never let the bounded ingest buffer fill: Submit() drops when full,
	// which is acceptable live-MQTT loss semantics but would silently lose
	// backfill rows. Wait for the writer to drain below the high-water
	// mark before submitting the next batch.
	const bufferHighWater = 2000

	connected := false
	backoff := time.Second

	for {
		query, args := buildPgBatchQuery(src, state, batchSize)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		rows, qerr := db.QueryContext(ctx, query, args...)
		if qerr != nil {
			cancel()
			status.MarkDisconnect(time.Now(), qerr)
			log.Printf("[pg:%s] query error: %v — retry in %s", tag, qerr, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		batch, serr := scanPgRows(rows)
		rows.Close()
		cancel()
		if serr != nil {
			status.MarkDisconnect(time.Now(), serr)
			log.Printf("[pg:%s] scan error: %v — retry in %s", tag, serr, backoff)
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

		if len(batch) == 0 {
			status.MarkPacket(time.Now())
			time.Sleep(pollInterval)
			continue
		}

		for ingestBuffer.Pending() > bufferHighWater {
			time.Sleep(25 * time.Millisecond)
		}

		droppedBefore := ingestBuffer.Dropped()
		batchStart := time.Now()
		for i := range batch {
			r := batch[i]
			env := pgEnvelope{
				Raw:       r.RawHex,
				Timestamp: r.Time.UTC().Format(time.RFC3339),
				SNR:       r.SNR,
				RSSI:      r.RSSI,
				Direction: r.Direction,
				Origin:    r.OriginName,
			}
			payload, merr := json.Marshal(env)
			if merr != nil {
				continue
			}
			m := &pgMessage{topic: r.Topic, payload: payload}
			ingestBuffer.Submit(func() {
				handleMessage(store, tag, srcForHandler, m, keySet.Snapshot(), regionSet, cfg)
			})
		}

		// Wait for the batch to fully drain before advancing the cursor:
		// the cursor must never point past rows that have not reached the
		// database, or a crash would skip them on resume.
		for ingestBuffer.Pending() > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		if d := ingestBuffer.Dropped() - droppedBefore; d > 0 {
			log.Printf("[pg:%s] WARNING: ingest buffer dropped %d row(s) — not advancing cursor, retrying batch", tag, d)
			time.Sleep(time.Second)
			continue
		}

		last := batch[len(batch)-1]
		state = pgSourceState{
			LastTime:  last.Time.UTC().Format(time.RFC3339Nano),
			LastObsID: last.ObsID,
		}
		if err := savePgSourceState(statePath, state); err != nil {
			log.Printf("[pg:%s] cursor save error (non-fatal; cursor held in memory): %v", tag, err)
		}
		status.MarkPacket(time.Now())

		elapsed := time.Since(batchStart)
		if src.MaxRowsPerSec > 0 {
			minDur := time.Duration(float64(len(batch)) / float64(src.MaxRowsPerSec) * float64(time.Second))
			if elapsed < minDur {
				time.Sleep(minDur - elapsed)
			}
		}
		if len(batch) < batchSize {
			// Caught up with the head of the table — switch to poll cadence.
			time.Sleep(pollInterval)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
