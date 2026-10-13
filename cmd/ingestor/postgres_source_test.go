package main

// Tests for the Postgres ingest source (postgres_source.go) and the
// historical-time plumbing it relies on (PreserveEnvelopeTime).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Compile-time check: pgMessage satisfies the same interface the MQTT
// delivery path uses.
var _ mqtt.Message = (*pgMessage)(nil)

func TestPgSourceStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pg-source-test.state.json")
	st := pgSourceState{LastTime: "2026-10-10T12:34:56.789Z", LastObsID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	if err := savePgSourceState(p, st); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok := loadPgSourceState(p)
	if !ok || got != st {
		t.Fatalf("round trip: got %+v ok=%v want %+v", got, ok, st)
	}
	// Missing file -> no cursor.
	if _, ok := loadPgSourceState(filepath.Join(dir, "missing.json")); ok {
		t.Fatalf("missing file must read as no-cursor")
	}
	// Corrupt file -> no cursor.
	if err := os.WriteFile(p, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadPgSourceState(p); ok {
		t.Fatalf("corrupt file must read as no-cursor")
	}
}

func TestBuildPgBatchQuery(t *testing.T) {
	cursor := pgSourceState{LastTime: "2026-10-01T00:00:00Z", LastObsID: pgZeroObsID}

	t.Run("defaults: public guard, no network filter", func(t *testing.T) {
		q, args := buildPgBatchQuery(PostgresSource{Name: "x"}, cursor, 500)
		if !strings.Contains(q, "meshcore_path_matches_private") || !strings.Contains(q, "visibility_generation = current_visibility.generation") {
			t.Errorf("public guard missing:\n%s", q)
		}
		if strings.Contains(q, "= ANY(") {
			t.Errorf("unexpected network filter:\n%s", q)
		}
		if len(args) != 3 { // time, obsID, limit
			t.Fatalf("args = %d, want 3 (%v)", len(args), args)
		}
		if args[2] != 500 {
			t.Errorf("limit arg = %v, want 500", args[2])
		}
		if !strings.Contains(q, "LIMIT $3") {
			t.Errorf("limit placeholder wrong:\n%s", q)
		}
	})

	t.Run("networks + visibility off", func(t *testing.T) {
		off := false
		src := PostgresSource{Name: "x", Networks: []string{"ukmesh", "northeast"}, RequirePublicVisibility: &off}
		q, args := buildPgBatchQuery(src, cursor, 250)
		if !strings.Contains(q, "= ANY($3)") || !strings.Contains(q, "LIMIT $4") {
			t.Errorf("placeholders wrong:\n%s", q)
		}
		if strings.Contains(q, "meshcore_path_matches_private") {
			t.Errorf("guard should be off:\n%s", q)
		}
		if len(args) != 4 {
			t.Fatalf("args = %d, want 4", len(args))
		}
	})
}

func TestF07LedgerCursorUsesCommitOrderRatherThanObservedTime(t *testing.T) {
	cursor := pgSourceState{LastTime: "2026-10-11T17:05:00Z", LastObsID: pgZeroObsID, LastSeq: 42}
	q, args := buildPgLedgerBatchQuery(PostgresSource{Networks: []string{"ukmesh"}}, cursor, 100)
	for _, want := range []string{"FROM ingest_observation_ledger l", "l.seq > $1", "ORDER BY l.seq", "c.observation_id::text", "WITH candidates AS MATERIALIZED", "CROSS JOIN LATERAL", "AND l.network = ANY($2)"} {
		if !strings.Contains(q, want) {
			t.Fatalf("ledger query missing %q: %s", want, q)
		}
	}
	if strings.Contains(q, "p.time >") || strings.Contains(q, "p.time,") && strings.Contains(q, "WHERE (p.time") {
		t.Fatalf("admission time must not gate a later commit: %s", q)
	}
	if args[0] != int64(42) || args[len(args)-1] != 100 {
		t.Fatalf("ledger cursor args = %v", args)
	}
	statePath := filepath.Join(t.TempDir(), "cursor.json")
	if err := savePgSourceState(statePath, pgSourceState{LastSeq: 43}); err != nil {
		t.Fatal(err)
	}
	got, ok := loadPgSourceState(statePath)
	if !ok || got.LastSeq != 43 {
		t.Fatalf("seq-only cursor not restored: %+v, %v", got, ok)
	}
}

func TestPgEnvelopeMarshal(t *testing.T) {
	snr := 5.5
	env := pgEnvelope{Raw: "0A00", Timestamp: "2026-10-01T00:00:00Z", SNR: &snr, Direction: "rx", Origin: "obs"}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`"raw":"0A00"`, `"timestamp":"2026-10-01T00:00:00Z"`, `"SNR":5.5`, `"direction":"rx"`, `"origin":"obs"`} {
		if !strings.Contains(s, want) {
			t.Errorf("envelope missing %s: %s", want, s)
		}
	}
	b2, _ := json.Marshal(pgEnvelope{Raw: "0A00"})
	if strings.Contains(string(b2), "RSSI") || strings.Contains(string(b2), "origin") {
		t.Errorf("empty optional fields must be omitted: %s", b2)
	}
}

// A historical source (PreserveEnvelopeTime) must store the envelope time as
// transmissions.first_seen / observations.timestamp / observer.last_seen —
// the inverse of the #1370/#1465 live-source behaviour, and the property a
// database backfill depends on to keep its timeline intact.
func TestHandleMessage_PreserveEnvelopeTime_Historical(t *testing.T) {
	store := newTestStore(t)
	source := MQTTSource{Name: "pg-test", PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}

	historical := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Second)
	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	payload := []byte(`{"raw":"` + rawHex + `","SNR":5.5,"RSSI":-100.0,"origin":"hist-obs","timestamp":"` + historical.Format(time.RFC3339) + `"}`)
	msg := &mockMessage{topic: "meshcore/MME/hist-obs/packets", payload: payload}

	handleMessage(store, "pg-test", source, msg, nil, nil, &Config{})

	var firstSeen string
	if err := store.db.QueryRow(`SELECT first_seen FROM transmissions LIMIT 1`).Scan(&firstSeen); err != nil {
		t.Fatalf("scan first_seen: %v", err)
	}
	fs, err := time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		t.Fatalf("first_seen %q: %v", firstSeen, err)
	}
	if fs.Unix() != historical.Unix() {
		t.Errorf("first_seen = %v, want %v (preserved envelope time)", fs, historical)
	}

	var obsTs int64
	if err := store.db.QueryRow(`SELECT timestamp FROM observations LIMIT 1`).Scan(&obsTs); err != nil {
		t.Fatalf("scan observations.timestamp: %v", err)
	}
	if obsTs != historical.Unix() {
		t.Errorf("observations.timestamp = %d, want %d", obsTs, historical.Unix())
	}

	var lastSeen string
	if err := store.db.QueryRow(`SELECT last_seen FROM observers WHERE id = ?`, "hist-obs").Scan(&lastSeen); err != nil {
		t.Fatalf("scan observer last_seen: %v", err)
	}
	ls, err := time.Parse(time.RFC3339, lastSeen)
	if err != nil {
		t.Fatalf("observer last_seen %q: %v", lastSeen, err)
	}
	if ls.Unix() != historical.Unix() {
		t.Errorf("observer.last_seen = %v, want %v (preserved)", ls, historical)
	}
}

func TestResolveRxTime_MaxPastDaysOverride(t *testing.T) {
	old := time.Now().UTC().Add(-45 * 24 * time.Hour).Truncate(time.Second)
	msg := map[string]interface{}{"timestamp": old.Format(time.RFC3339)}

	// Default (0 => 30d): a 45-day-old timestamp is clamped to now.
	got, _ := resolveRxTimeCore(msg, "t", 0)
	if time.Since(got) > time.Minute {
		t.Errorf("default clamp: got %v, want ~now", got)
	}

	// 60d window: the old timestamp is kept.
	got, _ = resolveRxTimeCore(msg, "t", 60)
	if got.Unix() != old.Unix() {
		t.Errorf("60d override: got %v, want %v", got, old)
	}

	// Negative: no past clamp at all.
	got, _ = resolveRxTimeCore(msg, "t", -1)
	if got.Unix() != old.Unix() {
		t.Errorf("unlimited override: got %v, want %v", got, old)
	}
}

func TestPgSourceDefaults(t *testing.T) {
	var p PostgresSource
	if p.PollIntervalOrDefault() != 5 {
		t.Errorf("poll default = %d", p.PollIntervalOrDefault())
	}
	if p.BatchSizeOrDefault() != 500 {
		t.Errorf("batch default = %d", p.BatchSizeOrDefault())
	}
	if !p.PublicVisibilityRequired() {
		t.Errorf("public visibility must default to required")
	}
	off := false
	p.RequirePublicVisibility = &off
	if p.PublicVisibilityRequired() {
		t.Errorf("explicit false must disable the guard")
	}
	if defaultPgStatePath("/cfg", "ukmesh-db") != "/cfg/pg-source-ukmesh-db.state.json" {
		t.Errorf("state path default wrong: %s", defaultPgStatePath("/cfg", "ukmesh-db"))
	}
	if redactPgDSN("postgres://user:secret@host:5432/db") != "postgres://user:***@host:5432/db" {
		t.Errorf("redact wrong: %s", redactPgDSN("postgres://user:secret@host:5432/db"))
	}
}
