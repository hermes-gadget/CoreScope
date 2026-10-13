package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// An explicitly configured disposable PostgreSQL fixture is required. Each
// test owns a schema; no existing source/database rows are changed.
func corePostgresFixture(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_CORE_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_CORE_DATABASE_URL is not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		t.Fatal("PostgreSQL fixture must use a loopback URL")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("core_f07_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	_, err = db.Exec(`
		CREATE TABLE packets (observation_id uuid, time timestamptz, topic text,
		  raw_hex text, snr double precision, rssi double precision, payload jsonb,
		  rx_node_id text, network text, src_node_id text, path_hashes text[], path_hash_size_bytes integer);
		CREATE TABLE nodes (public_key text, name text);
		CREATE TABLE ingest_observation_ledger (seq bigserial PRIMARY KEY,
		  observation_id uuid UNIQUE, observed_at timestamptz);
		CREATE TABLE ingest_observation_ledger_coverage (singleton boolean PRIMARY KEY, version integer);`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func readCoreLedgerFixture(t *testing.T, db *sql.DB, state pgSourceState) []pgRow {
	t.Helper()
	private := false // Authority filtering is tested separately under F01.
	query, args := buildPgLedgerBatchQuery(PostgresSource{RequirePublicVisibility: &private}, state, 100)
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	batch, err := scanPgRows(rows, true)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func ingestCorePGFixture(t *testing.T, store *Store, batch []pgRow) {
	t.Helper()
	for _, row := range batch {
		payload, err := json.Marshal(pgEnvelope{Raw: row.RawHex,
			Timestamp: row.Time.UTC().Format(time.RFC3339Nano), Direction: row.Direction})
		if err != nil {
			t.Fatal(err)
		}
		if err := handleMessage(store, "complete", MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1},
			&pgMessage{topic: row.Topic, payload: payload, obsID: row.ObsID}, nil, nil, &Config{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestF07RealLedgerHistoricalCoverageLateCommitAndCursorLoss(t *testing.T) {
	db := corePostgresFixture(t)
	const raw = "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	const oldID = "77777777-7777-4777-8777-777777777777"
	const lateID = "88888888-8888-4888-8888-888888888888"
	insert := func(id string, observed time.Time) {
		t.Helper()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec("SELECT pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'))"); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(`INSERT INTO packets (observation_id, time, topic, raw_hex, network)
		  VALUES ($1, $2, 'meshcore/ZZZ/complete-observer/packets', $3, 'ukmesh');`, id, observed, raw); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(`INSERT INTO ingest_observation_ledger (observation_id, observed_at) VALUES ($1, $2)`, id, observed); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	insert(oldID, time.Now().Add(-24*time.Hour))
	if batch := readCoreLedgerFixture(t, db, pgSourceState{}); len(batch) != 0 {
		t.Fatal("an incomplete historical backfill must not authorize a completeness cursor")
	}
	if _, err := db.Exec(`INSERT INTO ingest_observation_ledger_coverage VALUES (TRUE, 1)`); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	batch := readCoreLedgerFixture(t, db, pgSourceState{})
	if len(batch) != 1 || batch[0].ObsID != oldID {
		t.Fatalf("historical coverage: %+v", batch)
	}
	ingestCorePGFixture(t, store, batch)
	cursorPath := filepath.Join(t.TempDir(), "cursor.json")
	cursor := pgSourceState{LastSeq: batch[0].Seq}
	if err := savePgSourceState(cursorPath, cursor); err != nil {
		t.Fatal(err)
	}
	// Late commit is older than all consumed observations, independent of
	// elapsed Redis retention. The fixture has no Redis copy to fall back to.
	insert(lateID, time.Now().Add(-48*time.Hour))
	batch = readCoreLedgerFixture(t, db, cursor)
	if len(batch) != 1 || batch[0].ObsID != lateID {
		t.Fatalf("late observation skipped: %+v", batch)
	}
	ingestCorePGFixture(t, store, batch)
	if err := os.Remove(cursorPath); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadPgSourceState(cursorPath); ok {
		t.Fatal("lost cursor unexpectedly restored")
	}
	ingestCorePGFixture(t, store, readCoreLedgerFixture(t, db, pgSourceState{}))
	var count, identities int
	if err := store.db.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT id) FROM observations`).Scan(&count, &identities); err != nil {
		t.Fatal(err)
	}
	if count != 2 || identities != 2 {
		t.Fatalf("cursor reset duplicated or lost observations: count=%d identities=%d", count, identities)
	}
}
