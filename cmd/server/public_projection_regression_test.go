package main

import (
	"encoding/json"
	"testing"
	"time"
)

// TestProjectionRouteBuildSurvivesIndexesNotReady is a regression test for
// the ukmesh reload loop (2026-10-22). routesFor used to return an error when
// the background subpath index had not finished inside a fixed 5s budget,
// discarding the freshly loaded store. Every following request then cold-loaded
// the entire DB again, so the instance never served a request and pinned all
// cores. The route table must be installed regardless; handlers gate the few
// endpoints that need the indexes behind 503 + Retry-After.
func TestProjectionRouteBuildSurvivesIndexesNotReady(t *testing.T) {
	oldAwait := awaitProjectionIndexes
	awaitProjectionIndexes = func(*PacketStore, time.Duration) bool { return false }
	defer func() { awaitProjectionIndexes = oldAwait }()

	db := setupTestDB(t)
	seedTestData(t, db)

	var cfg Config
	raw, _ := json.Marshal(struct {
		PublicProjection bool `json:"publicProjection"`
	}{true})
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, &cfg, NewHub())
	parent := NewPacketStore(db, nil)
	if err := parent.Load(); err != nil {
		t.Fatal(err)
	}
	parent.WaitIndexesReady(time.Second)
	srv.store = parent

	reader := newPublicProjectionReader(srv)
	routes, err := reader.routesFor(7)
	if err != nil {
		t.Fatalf("routesFor failed when the index wait reported not-ready: %v", err)
	}
	if routes == nil {
		t.Fatal("routesFor returned a nil router")
	}
	if reader.generation != 7 || reader.routes == nil {
		t.Fatalf("route generation was not installed: generation=%d routes_set=%v", reader.generation, reader.routes != nil)
	}
	// A second call for the same generation must reuse the cached router
	// rather than cold-load the store again.
	reused, err := reader.routesFor(7)
	if err != nil {
		t.Fatalf("cached routesFor failed: %v", err)
	}
	if reused != routes {
		t.Fatal("same-generation call rebuilt the route table instead of reusing it")
	}
}
