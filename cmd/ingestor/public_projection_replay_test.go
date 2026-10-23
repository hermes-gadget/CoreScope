package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func applyReplayFixture(store *Store, record publicProjectionRecord) error {
	return handleMessage(store, record.Tag, record.Source,
		&pgMessage{topic: record.Topic, payload: record.Payload, obsID: record.ObservationID}, nil, nil, &Config{})
}

type projectionReplayCrashFixture struct{ Database, Marker, Cursor string }

func TestProjectionReplayCrashWorker(t *testing.T) {
	path := os.Getenv("CORE_PROJECTION_REPLAY_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture projectionReplayCrashFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(fixture.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return &corePublicPolicy{generation: 2, fingerprint: "changed"}, nil
	}, func(target *Store, record publicProjectionRecord) error {
		if err := applyReplayFixture(target, record); err != nil {
			return err
		}
		if record.ObservationID == "00000000-0000-4000-8000-000000000258" {
			if err := os.WriteFile(fixture.Marker, []byte("inside uncommitted page two"), 0o600); err != nil {
				return err
			}
			select {} // the parent kills this disposable worker before commit
		}
		return nil
	}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := savePgSourceState(fixture.Cursor, pgSourceState{LastSeq: 999}); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionReplayProcessCrashKeepsCursorAndRecovers(t *testing.T) {
	resetScopeMatchCounters(t)
	dir := t.TempDir()
	fixture := projectionReplayCrashFixture{filepath.Join(dir, "crash.db"), filepath.Join(dir, "active"), filepath.Join(dir, "cursor.json")}
	store, err := OpenStore(fixture.Database)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return &corePublicPolicy{generation: 1, fingerprint: "initial"}, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedProjectionReplay(t, store, 515)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	safe := pgSourceState{PolicyGeneration: 1, PolicyFingerprint: "initial", LastSeq: 777}
	if err := savePgSourceState(fixture.Cursor, safe); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "input.json")
	body, _ := json.Marshal(fixture)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run=^TestProjectionReplayCrashWorker$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "CORE_PROJECTION_REPLAY_CRASH_FIXTURE="+path)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(fixture.Marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never reached an uncommitted replay page")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("fixture worker did not crash")
	}
	if cursor, ok := loadPgSourceState(fixture.Cursor); !ok || cursor != safe {
		t.Fatalf("crash advanced cursor: %+v ok=%v", cursor, ok)
	}
	store, err = OpenStore(fixture.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var observations, ready int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&observations); err != nil || observations != 256 {
		t.Fatalf("crash exposed a partial page: observations=%d err=%v", observations, err)
	}
	if err := store.db.QueryRow(`SELECT ready FROM public_projection_state`).Scan(&ready); err != nil || ready != 0 {
		t.Fatalf("crash published ready=%d err=%v", ready, err)
	}
	projection, err = newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return &corePublicPolicy{generation: 2, fingerprint: "changed"}, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&observations); err != nil || observations != 515 {
		t.Fatalf("crash recovery observations=%d err=%v", observations, err)
	}
}

func TestProjectionReplayRestoresDurabilityAfterCancellationAndPanic(t *testing.T) {
	for _, mode := range []string{"cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "rollback.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.backfillWg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var failure error
			var panicked bool
			func() {
				defer func() { panicked = recover() != nil }()
				failure = store.withProjectionReplayTx(ctx, func(target *Store) error {
					if _, err := target.WriterExec("fixture", `INSERT INTO nodes(public_key,name) VALUES('fixture-node','rollback')`); err != nil {
						return err
					}
					if err := target.WriterTx("fixture", func(tx *sql.Tx) error {
						_, err := tx.Exec(`UPDATE nodes SET name='still-rollback' WHERE public_key='fixture-node'`)
						return err
					}); err != nil {
						return err
					}
					if mode == "panic" {
						panic("fixture panic")
					}
					cancel()
					return ctx.Err()
				})
			}()
			if (mode == "panic" && !panicked) || (mode == "cancel" && !errors.Is(failure, context.Canceled)) {
				t.Fatalf("mode=%s panic=%v err=%v", mode, panicked, failure)
			}
			var count, synchronous int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rolled-back node count=%d err=%v", count, err)
			}
			if err := store.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil || synchronous != 2 {
				t.Fatalf("live synchronous=%d err=%v", synchronous, err)
			}
		})
	}
}

// Seed the private journal directly so the measurement covers the real replay
// decoder and all derived writes, rather than the live source admission path.
func seedProjectionReplay(t testing.TB, store *Store, count int) {
	t.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO public_projection_journal(observation_id,observed_at,record) VALUES(?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	stamp := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	payload, _ := json.Marshal(pgEnvelope{Raw: "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976", Timestamp: stamp})
	for i := 1; i <= count; i++ {
		record := publicProjectionRecord{ObservationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
			Topic: "meshcore/ZZZ/replay-fixture/packets", Payload: payload, Network: "ukmesh", Tag: "replay",
			Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}}
		if i == 257 {
			record.RXNodeID = "private-fixture"
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.Exec(record.ObservationID, stamp, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionReplayRollsBackWholePageAndRecovers(t *testing.T) {
	resetScopeMatchCounters(t)
	store, err := OpenStore(filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	policy := &corePublicPolicy{generation: 1, fingerprint: "initial"}
	fail := false
	apply := func(target *Store, record publicProjectionRecord) error {
		var synchronous int
		if err := target.ingestDB.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil || synchronous != 1 {
			t.Fatalf("replay synchronous=%d err=%v, want NORMAL", synchronous, err)
		}
		if err := handleMessage(target, record.Tag, record.Source, &pgMessage{topic: record.Topic, payload: record.Payload, obsID: record.ObservationID}, nil, nil, &Config{}); err != nil {
			return err
		}
		if fail && record.ObservationID == "00000000-0000-4000-8000-000000000258" {
			return errors.New("failure after derived writes")
		}
		return nil
	}
	load := func(context.Context) (*corePublicPolicy, error) { return policy, nil }
	projection, err := newCorePublicProjection(store, load, apply, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedProjectionReplay(t, store, 515)
	policy = &corePublicPolicy{generation: 2, fingerprint: "restricted", prefixes: []corePrivatePrefix{{NodeID: "private-fixture", Network: "ukmesh", Size: 1, Prefix: "AA"}}}
	fail = true
	if _, err := projection.Refresh(context.Background()); err == nil {
		t.Fatal("failed replay succeeded")
	}
	assertCounts := func(observations, journal, ready int) {
		t.Helper()
		for _, check := range []struct {
			query string
			want  int
		}{
			{`SELECT COUNT(*) FROM observations`, observations},
			{`SELECT COUNT(*) FROM public_projection_journal`, journal},
			{`SELECT ready FROM public_projection_state`, ready},
			{`PRAGMA synchronous`, 2},
		} {
			var got int
			if err := store.db.QueryRow(check.query).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != check.want {
				t.Fatalf("%s = %d, want %d", check.query, got, check.want)
			}
		}
	}
	// Page two must roll back both derived writes and private journal deletion.
	assertCounts(256, 515, 0)
	fail = false
	projection, err = newCorePublicProjection(store, load, apply, 30)
	if err != nil {
		t.Fatal(err)
	}
	before := store.Stats.WALCommits.Load()
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCounts(514, 514, 1)
	var packets int
	if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id='replay-fixture'`).Scan(&packets); err != nil || packets != 514 {
		t.Fatalf("retry observer count=%d err=%v", packets, err)
	}
	if commits := store.Stats.WALCommits.Load() - before; commits != 3 {
		t.Fatalf("replayed 515 records in %d commits, want 3 bounded page commits", commits)
	}
	// A completed restart trusts ready=1, retaining the b7 no-replay behavior.
	projection, err = newCorePublicProjection(store, load, func(*Store, publicProjectionRecord) error {
		t.Fatal("completed projection was replayed on restart")
		return nil
	}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkProjectionReplay(b *testing.B) {
	store, err := OpenStore(filepath.Join(b.TempDir(), "replay.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	policy := &corePublicPolicy{generation: 1, fingerprint: "benchmark"}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) { return policy, nil },
		applyReplayFixture, 30)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	seedProjectionReplay(b, store, 2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.db.Exec(`UPDATE public_projection_state SET ready=0`); err != nil {
			b.Fatal(err)
		}
		if _, err := projection.Refresh(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	var observations int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&observations); err != nil || observations != 2048 {
		b.Fatalf("replay observations=%d err=%v", observations, err)
	}
}
