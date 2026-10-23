package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type coreSourceFixtureMessage struct {
	Topic, Payload, ObservationID string
}

type coreCrashFixture struct {
	Messages                                             []coreSourceFixtureMessage
	Order                                                []string
	Database, PGCursor, RedisCursor, Marker, LastRedisID string
}

func coreFixtureSave(f coreCrashFixture, source string) func() error {
	return func() error {
		if source == "postgres" {
			return savePgSourceState(f.PGCursor, pgSourceState{LastSeq: 2})
		}
		return saveRedisSourceState(f.RedisCursor, redisSourceState{LastID: f.LastRedisID})
	}
}

func coreFixtureJobs(t *testing.T, buffer *IngestBuffer, store *Store, messages []coreSourceFixtureMessage,
	beforeLast func()) []<-chan error {
	t.Helper()
	completed := make([]<-chan error, 0, len(messages))
	for i, message := range messages {
		i, message := i, message
		result, accepted := buffer.SubmitTracked(func() error {
			if beforeLast != nil && i == len(messages)-1 {
				beforeLast()
			}
			if store.publicProjection != nil {
				return store.publicProjection.Batch(context.Background(), []publicProjectionRecord{{
					ObservationID: message.ObservationID, Topic: message.Topic, Payload: []byte(message.Payload), Network: "ukmesh",
					Tag: "complete", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1},
				}})
			}
			return handleMessage(store, "complete", MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1},
				&pgMessage{topic: message.Topic, payload: []byte(message.Payload), obsID: message.ObservationID}, nil, nil, &Config{})
		})
		if !accepted {
			t.Fatal("fixture batch unexpectedly refused")
		}
		completed = append(completed, result)
	}
	return completed
}

func installCoreFixtureProjection(t *testing.T, store *Store) {
	t.Helper()
	projection, err := newCorePublicProjection(store,
		func(context.Context) (*corePublicPolicy, error) { return &corePublicPolicy{generation: 1}, nil },
		applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	store.publicProjection = projection
}

// This helper runs only in a child of the integration test. The parent kills
// this disposable test process at the exact active-last-write boundary.
func TestF08CrashWorker(t *testing.T) {
	path := os.Getenv("CORE_F08_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f coreCrashFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(f.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	installCoreFixtureProjection(t, store)
	buffer := NewIngestBuffer(10)
	buffer.Start()
	buffer.Ready()
	for _, source := range f.Order {
		jobs := coreFixtureJobs(t, buffer, store, f.Messages, func() {
			if buffer.Pending() != 0 {
				panic("expected last job to be active with an empty queue")
			}
			if err := os.WriteFile(f.Marker, []byte("ACTIVE-LAST-WRITE"), 0o600); err != nil {
				panic(err)
			}
			select {} // killed by the parent before this write can complete
		})
		if err := finishSourceBatch(jobs, false, coreFixtureSave(f, source)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestF08RealPairedSourcesCrashFailureAndRestartBothOrders(t *testing.T) {
	resetScopeMatchCounters(t)
	redisURL := os.Getenv("TEST_CORE_REDIS_URL")
	u, err := url.Parse(redisURL)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		t.Skip("TEST_CORE_REDIS_URL must use a loopback fixture")
	}
	db := corePostgresFixture(t)
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx := context.Background()
	key := fmt.Sprintf("core:f08:%d:%d", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() { client.Del(ctx, key) })
	const frame = "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	const topic = "meshcore/ZZZ/paired-observer/packets"
	ids := []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"}
	redisMessages := make([]coreSourceFixtureMessage, 0, 2)
	lastID := ""
	for _, id := range ids {
		// Both sources carry the same PostgreSQL-supported timestamp precision.
		observed := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
		if _, err := db.Exec(`INSERT INTO packets (observation_id, time, topic, raw_hex, network)
		  VALUES ($1, $2, $3, $4, 'ukmesh')`, id, observed, topic, frame); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO ingest_observation_ledger (observation_id, observed_at) VALUES ($1, $2)`, id, observed); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]interface{}{"v": 1, "k": "p", "o": id, "vo": 1,
			"d": map[string]interface{}{"time": observed.Format(time.RFC3339Nano), "topic": topic, "rawHex": frame, "network": "ukmesh"}})
		if err != nil {
			t.Fatal(err)
		}
		lastID, err = client.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"e": string(body)}}).Result()
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO ingest_observation_ledger_coverage VALUES (TRUE, 1)`); err != nil {
		t.Fatal(err)
	}
	stream, err := client.XRange(ctx, key, "-", "+").Result()
	if err != nil || len(stream) != 2 {
		t.Fatalf("Redis read: %v, count=%d", err, len(stream))
	}
	for _, item := range stream {
		entry, ok := decodeRedisStreamValue(item.Values["e"].(string))
		if !ok {
			t.Fatal("real Redis delivery failed decoding")
		}
		payload, _ := json.Marshal(pgEnvelope{Raw: entry.rawHex, Timestamp: entry.time})
		redisMessages = append(redisMessages, coreSourceFixtureMessage{entry.topic, string(payload), entry.observationID})
	}
	pgMessages := make([]coreSourceFixtureMessage, 0, 2)
	for _, row := range readCoreLedgerFixture(t, db, pgSourceState{}) {
		payload, _ := json.Marshal(pgEnvelope{Raw: row.RawHex, Timestamp: row.Time.UTC().Format(time.RFC3339Nano), SNR: row.SNR})
		pgMessages = append(pgMessages, coreSourceFixtureMessage{row.Topic, string(payload), row.ObsID})
	}
	for _, order := range [][]string{{"postgres", "redis"}, {"redis", "postgres"}} {
		t.Run(strings.Join(order, "-first-"), func(t *testing.T) {
			dir := t.TempDir()
			messages := map[string][]coreSourceFixtureMessage{"postgres": pgMessages, "redis": redisMessages}
			f := coreCrashFixture{Messages: messages[order[0]], Order: order, Database: filepath.Join(dir, "copy.db"),
				PGCursor: filepath.Join(dir, "pg.json"), RedisCursor: filepath.Join(dir, "redis.json"),
				Marker: filepath.Join(dir, "active"), LastRedisID: lastID}
			safePG := pgSourceState{LastTime: time.Now().Format(time.RFC3339Nano)}
			safeRedis := redisSourceState{LastID: "0-0"}
			if err := savePgSourceState(f.PGCursor, safePG); err != nil {
				t.Fatal(err)
			}
			if err := saveRedisSourceState(f.RedisCursor, safeRedis); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "input.json")
			body, _ := json.Marshal(f)
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(binary, "-test.run=^TestF08CrashWorker$", "-test.timeout=30s")
			child.Env = append(os.Environ(), "CORE_F08_CRASH_FIXTURE="+path)
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer child.Process.Kill()
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(f.Marker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child never reached the active-last-write boundary")
				}
				time.Sleep(10 * time.Millisecond)
			}
			assertSafe := func() {
				t.Helper()
				pg, ok := loadPgSourceState(f.PGCursor)
				if !ok || pg != safePG {
					t.Fatalf("PG cursor passed incomplete writes: %+v", pg)
				}
				r, ok := loadRedisSourceState(f.RedisCursor)
				if !ok || r != safeRedis {
					t.Fatalf("Redis cursor passed incomplete writes: %+v", r)
				}
			}
			assertSafe()
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := child.Wait(); err == nil {
				t.Fatal("fixture process did not crash")
			}
			assertSafe()
			store, err := OpenStore(f.Database) // reopen the actual crashed SQLite/WAL
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			readyCtx, cancelReady := context.WithTimeout(ctx, 5*time.Second)
			defer cancelReady()
			if err := waitForUpstreamIdentityReady(readyCtx, store); err != nil {
				t.Fatal(err)
			}
			installCoreFixtureProjection(t, store)
			buffer := NewIngestBuffer(10)
			buffer.Start()
			buffer.Ready()
			defer buffer.Stop()
			if _, err := store.db.Exec(`CREATE TRIGGER f08_failure BEFORE INSERT ON observations
				WHEN NEW.upstream_observation_id = '` + ids[1] + `'
				BEGIN SELECT RAISE(FAIL, 'F08 SQLite failure'); END`); err != nil {
				t.Fatal(err)
			}
			for _, source := range order {
				jobs := coreFixtureJobs(t, buffer, store, messages[source], nil)
				if err := finishSourceBatch(jobs, false, coreFixtureSave(f, source)); err == nil {
					t.Fatal("source cursor advanced after a real SQLite write failure")
				}
				assertSafe()
			}
			if _, err := store.db.Exec(`DROP TRIGGER f08_failure`); err != nil {
				t.Fatal(err)
			}
			for _, source := range order {
				jobs := coreFixtureJobs(t, buffer, store, messages[source], nil)
				if err := finishSourceBatch(jobs, false, coreFixtureSave(f, source)); err != nil {
					t.Fatal(err)
				}
			}
			var observations, counts int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&observations); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id = 'paired-observer'`).Scan(&counts); err != nil {
				t.Fatal(err)
			}
			if observations != 2 || counts != 2 {
				t.Fatalf("paired replay observations=%d count=%d", observations, counts)
			}
			pg, _ := loadPgSourceState(f.PGCursor)
			r, _ := loadRedisSourceState(f.RedisCursor)
			if pg.LastSeq != 2 || r.LastID != lastID {
				t.Fatalf("completed source cursors: %+v %+v", pg, r)
			}
		})
	}
}
