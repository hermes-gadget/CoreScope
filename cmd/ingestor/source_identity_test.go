package main

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestF17HistoricalSourcesWaitForIdentityIndexMigration(t *testing.T) {
	store := newTestStore(t)
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DROP INDEX idx_observations_upstream_id`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForUpstreamIdentityReady(ctx, store); err != context.Canceled {
		t.Fatalf("source should hold cursor while identity index is unavailable: %v", err)
	}
}

func TestF17TwoSourceObservationIdentityAndCountInBothOrders(t *testing.T) {
	for _, order := range []string{"redis-first", "postgres-first"} {
		t.Run(order, func(t *testing.T) {
			store := newTestStore(t)
			source := MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}
			obsID := "f47ac10b-58cc-4372-a567-0e02b2c3d479"
			timestamp := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
			raw := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
			redis := &pgMessage{topic: "meshcore/MME/test-observer/packets", obsID: obsID,
				payload: []byte(`{"raw":"` + raw + `","timestamp":"` + timestamp + `"}`)}
			postgres := &pgMessage{topic: redis.topic, obsID: obsID,
				payload: []byte(`{"raw":"` + raw + `","timestamp":"` + timestamp + `","SNR":5.5}`)}
			messages := []*pgMessage{redis, postgres}
			if order == "postgres-first" {
				messages = []*pgMessage{postgres, redis}
			}
			for _, message := range messages {
				if err := handleMessage(store, "source", source, message, nil, nil, &Config{}); err != nil {
					t.Fatal(err)
				}
			}
			var observations, count int
			var storedID string
			var snr sql.NullFloat64
			if err := store.db.QueryRow(`SELECT COUNT(*), MIN(upstream_observation_id), MAX(snr) FROM observations`).Scan(
				&observations, &storedID, &snr); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id = ?`, "test-observer").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if observations != 1 || count != 1 || storedID != obsID || !snr.Valid || snr.Float64 != 5.5 {
				t.Fatalf("observations=%d count=%d id=%q snr=%v", observations, count, storedID, snr)
			}
		})
	}
}
