package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPgPolicyCursorSeeksRetentionOnlyForChangedRules(t *testing.T) {
	cutoff := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		generation  int64
		fingerprint string
		wantSeq     int64
		calls       int
	}{
		{"widened policy", 1, "more-private", 41, 1},
		{"narrowed policy", 1, "less-private", 41, 1},
		{"unchanged fingerprint", 1, "current", 900, 0},
		{"same generation", 2, "current", 900, 0},
		{"legacy state", 1, "", 900, 0},
		{"fresh state", 0, "", 900, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := pgSourceState{PolicyGeneration: tc.generation, PolicyFingerprint: tc.fingerprint,
				LastSeq: 900, LastTime: "retained-time", LastObsID: "retained-id"}
			calls := 0
			next, err := pgPolicyCursor(state, 2, "current", cutoff, func(got time.Time) (int64, error) {
				calls++
				if !got.Equal(cutoff) {
					t.Fatalf("seek cutoff=%s want=%s", got, cutoff)
				}
				return 41, nil
			})
			if err != nil || calls != tc.calls || next.LastSeq != tc.wantSeq || next.PolicyGeneration != 2 || next.PolicyFingerprint != "current" || next.LastTime != state.LastTime || next.LastObsID != state.LastObsID {
				t.Fatalf("next=%+v calls=%d err=%v", next, calls, err)
			}
		})
	}
	state := pgSourceState{PolicyGeneration: 1, PolicyFingerprint: "old", LastSeq: 900}
	failed := errors.New("seek unavailable")
	next, err := pgPolicyCursor(state, 2, "current", cutoff, func(time.Time) (int64, error) { return 0, failed })
	if !errors.Is(err, failed) || next != state {
		t.Fatalf("failed seek changed cursor: next=%+v err=%v", next, err)
	}
}

func TestPgRetentionSeekQueryUsesEarliestEligibleSequence(t *testing.T) {
	cutoff := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	query, args := buildPgRetentionCursorQuery(PostgresSource{}, cutoff)
	for _, want := range []string{"MIN(seq) - 1", "MAX(seq)", "observed_at >= $1"} {
		if !strings.Contains(query, want) {
			t.Fatalf("retention query missing %q: %s", want, query)
		}
	}
	if !reflect.DeepEqual(args, []interface{}{cutoff}) {
		t.Fatalf("retention args=%v", args)
	}
	query, args = buildPgRetentionCursorQuery(PostgresSource{Networks: []string{"fixture-net"}}, cutoff)
	if !strings.Contains(query, "network = ANY($2)") || len(args) != 2 {
		t.Fatalf("network scope lost: %s args=%v", query, args)
	}
}

func TestPgRetentionCursorRealLedgerBoundariesAndOutOfOrderTime(t *testing.T) {
	db := corePostgresFixture(t)
	if _, err := db.Exec(`ALTER TABLE ingest_observation_ledger ADD COLUMN network text NOT NULL DEFAULT 'fixture-net'`); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	seek := func(src PostgresSource, want int64) {
		t.Helper()
		query, args := buildPgRetentionCursorQuery(src, cutoff)
		var got int64
		if err := db.QueryRow(query, args...).Scan(&got); err != nil || got != want {
			t.Fatalf("retention floor=%d want=%d err=%v", got, want, err)
		}
	}
	seek(PostgresSource{}, 0) // no ledger
	// Retained timestamps are deliberately interleaved with expired records:
	// MAX(expired seq) would skip the earlier retained observation at seq=20.
	for i, stamp := range []time.Time{cutoff.Add(-time.Hour), cutoff, cutoff.Add(-time.Second), cutoff.Add(time.Hour)} {
		if _, err := db.Exec(`INSERT INTO ingest_observation_ledger(seq,observation_id,observed_at) VALUES($1,$2,$3)`,
			10*(i+1), "00000000-0000-4000-8000-"+[]string{"000000000001", "000000000002", "000000000003", "000000000004"}[i], stamp); err != nil {
			t.Fatal(err)
		}
	}
	seek(PostgresSource{}, 19) // includes the exact cutoff boundary at seq=20
	seek(PostgresSource{Networks: []string{"absent-network"}}, 40)
	if _, err := db.Exec(`UPDATE ingest_observation_ledger SET observed_at=$1`, cutoff.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	seek(PostgresSource{}, 40) // all expired: follow future committed sequences
}
