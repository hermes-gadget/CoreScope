package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestF16PublicLiveSourceUsesCredentialsAndRejectsUnsafeCursorBatches(t *testing.T) {
	batch := publicLiveBatch{Generation: 2, NextCursor: "20-0", Entries: []publicLiveEntry{{ID: "19-0", Value: "fixture"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture" {
			http.Error(w, "denied", 401)
			return
		}
		if r.URL.Query().Get("after") != "10-0" || r.URL.Query().Get("limit") != "500" {
			t.Error("public source cursor/limit missing")
		}
		json.NewEncoder(w).Encode(batch)
	}))
	defer server.Close()
	source := RedisSource{PublicURL: server.URL, PublicToken: "fixture", BatchSize: 2000}
	client := server.Client()
	if _, err := fetchPublicLiveBatch(context.Background(), client, source, "10-0"); err != nil {
		t.Fatal(err)
	}
	batch.NextCursor = "9-0"
	if _, err := fetchPublicLiveBatch(context.Background(), client, source, "10-0"); err == nil {
		t.Fatal("backwards cursor accepted")
	}
	batch.NextCursor = "20-0"
	batch.Entries[0].ID = "21-0"
	if _, err := fetchPublicLiveBatch(context.Background(), client, source, "10-0"); err == nil {
		t.Fatal("observation beyond cursor accepted")
	}
	source.PublicToken = "wrong"
	if _, err := fetchPublicLiveBatch(context.Background(), client, source, "10-0"); err == nil {
		t.Fatal("unauthorized public response accepted")
	}
}
