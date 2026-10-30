package main

import (
	"errors"
	"testing"
	"time"
)

func TestF08SourceBatchWaitsForLastJobAndReturnsWriteFailure(t *testing.T) {
	want := errors.New("sqlite unavailable")
	first := make(chan error, 1)
	last := make(chan error, 1)
	first <- want
	completed := make(chan error, 1)
	go func() {
		completed <- waitTrackedIngestJobs([]<-chan error{first, last})
	}()
	select {
	case <-completed:
		t.Fatal("batch completed before its active final job")
	default:
	}
	last <- nil
	select {
	case err := <-completed:
		if !errors.Is(err, want) {
			t.Fatalf("write failure was not propagated: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("batch completion did not return")
	}
}

func TestF08HistoricalPacketHandlerPropagatesSQLiteFailure(t *testing.T) {
	store := newTestStore(t)
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	msg := &mockMessage{
		topic:   "meshcore/SJC/observer/packets",
		payload: []byte(`{"raw":"0A00D69FD7A5A7475DB07337749AE61FA53A4788E976","SNR":5.5,"RSSI":-100.0}`),
	}
	err := handleMessage(store, "pg-test", MQTTSource{Name: "pg-test", PreserveEnvelopeTime: true}, msg, nil, nil, &Config{})
	if err == nil {
		t.Fatal("historical source handler swallowed a SQLite write failure")
	}
}

func TestF08HistoricalPacketHandlerPropagatesObservationInsertFailure(t *testing.T) {
	store := newTestStore(t)
	if _, err := store.db.Exec(`CREATE TRIGGER f08_reject_observation
		BEFORE INSERT ON observations BEGIN SELECT RAISE(FAIL, 'injected observation failure'); END`); err != nil {
		t.Fatal(err)
	}
	msg := &mockMessage{
		topic:   "meshcore/SJC/observer/packets",
		payload: []byte(`{"raw":"0A00D69FD7A5A7475DB07337749AE61FA53A4788E976","SNR":5.5,"RSSI":-100.0}`),
	}
	err := handleMessage(store, "pg-test", MQTTSource{Name: "pg-test", PreserveEnvelopeTime: true}, msg, nil, nil, &Config{})
	if err == nil {
		t.Fatal("historical source reported completion after observation insert failed")
	}
}
