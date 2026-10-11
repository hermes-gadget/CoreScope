package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestF01RealAuthorityRevokesCopiedObserverSourceAndAllPrefixWidths(t *testing.T) {
	resetScopeMatchCounters(t)
	authority := corePostgresFixture(t)
	_, err := authority.Exec(`CREATE TABLE public_visibility_state(singleton boolean PRIMARY KEY,generation bigint);
 CREATE TABLE packet_visibility_materialization_state(singleton boolean PRIMARY KEY,visibility_generation bigint);
 CREATE TABLE privacy_rematerialization_queue(status text);
 CREATE TABLE private_node_prefixes(node_id text,network text,prefix_size_bytes integer,prefix text);
 INSERT INTO public_visibility_state VALUES(TRUE,1);
 INSERT INTO packet_visibility_materialization_state VALUES(TRUE,1)`)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "public.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	projection, err := newCorePublicProjection(store, func(ctx context.Context) (*corePublicPolicy, error) { return loadCorePublicPolicy(ctx, authority) },
		func(record publicProjectionRecord) error {
			return handleMessage(store, record.Tag, record.Source, &pgMessage{topic: record.Topic, payload: record.Payload, obsID: record.ObservationID}, nil, nil, &Config{})
		}, 30)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(pgEnvelope{Raw: "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	privateNode := "AABBCC" + strings.Repeat("D", 58)
	records := make([]publicProjectionRecord, 7)
	for i := range records {
		records[i] = publicProjectionRecord{ObservationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), Topic: "meshcore/ZZZ/revocation-observer/packets", Payload: payload, Network: "northeast", Tag: "paired", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}}
	}
	records[0].RXNodeID = privateNode
	records[1].SrcNodeID = privateNode
	for i, path := range []string{"AA", "AABB", "AABBCC"} {
		records[i+2].PathHashes = []string{path}
		records[i+2].PathHashSizeBytes = i + 1
	}
	records[5].RXNodeID = privateNode
	records[5].Network = "othernet"
	if err := projection.Batch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	check := func(want int) {
		t.Helper()
		var observations, journal, count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&observations); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM public_projection_journal`).Scan(&journal); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id='revocation-observer'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if observations != want || journal != want || count != want {
			t.Fatalf("observations=%d journal=%d derived=%d want=%d", observations, journal, count, want)
		}
	}
	check(7)
	if _, err := authority.Exec(`INSERT INTO privacy_rematerialization_queue VALUES('processing');UPDATE public_visibility_state SET generation=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err == nil {
		t.Fatal("rematerializing authority accepted")
	}
	for i, path := range []string{"AA", "AABB", "AABBCC"} {
		if _, err := authority.Exec(`INSERT INTO private_node_prefixes VALUES($1,'teesside',$2,$3)`, privateNode, i+1, path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := authority.Exec(`DELETE FROM privacy_rematerialization_queue;UPDATE packet_visibility_materialization_state SET visibility_generation=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(2) // one unrelated network and one public observation remain
	if _, err := authority.Exec(`ALTER TABLE public_visibility_state RENAME TO held_authority`); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err == nil {
		t.Fatal("unavailable authority accepted")
	}
}
