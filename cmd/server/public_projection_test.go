package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

func TestF01CopiedProjectionIsFencedAndCachesReplacedOnRevocation(t *testing.T) {
	var generation atomic.Int64
	generation.Store(1)
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(w, "denied", 401)
			return
		}
		if generation.Load() < 1 {
			http.Error(w, "unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Generation int64 `json:"generation"`
		}{generation.Load()})
	}))
	defer authority.Close()
	db := setupTestDB(t)
	seedTestData(t, db)
	if _, err := db.conn.Exec(`CREATE TABLE public_projection_state(singleton INTEGER PRIMARY KEY,
   generation INTEGER NOT NULL, ready INTEGER NOT NULL);
   INSERT INTO public_projection_state VALUES(1, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	raw, _ := json.Marshal(struct {
		PublicProjection     bool   `json:"publicProjection"`
		PublicAuthorityURL   string `json:"publicAuthorityURL"`
		PublicAuthorityToken string `json:"publicAuthorityToken"`
	}{true, authority.URL, "fixture-token"})
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, &cfg, NewHub())
	store := NewPacketStore(db, nil)
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	request := func(path string, code int) string {
		t.Helper()
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != code {
			t.Fatalf("%s: status=%d want=%d body=%s", path, response.Code, code, response.Body.String())
		}
		return response.Body.String()
	}
	if !strings.Contains(request("/api/nodes", 200), "TestRepeater") {
		t.Fatal("fixture copy missing")
	}
	request("/api/packets", 200) // populate caches before the privacy transition
	generation.Store(2)
	request("/api/packets", 503)
	request("/api/nodes", 503)
	request("/api/stats", 503)
	// Only the ingestor writes the measurement DB in production. This test's
	// fixture connection stands in for its completed selective rebuild.
	if _, err := db.conn.Exec(`DELETE FROM observations; DELETE FROM transmissions;
   DELETE FROM nodes; DELETE FROM observers;
   UPDATE public_projection_state SET generation=2, ready=1 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(request("/api/nodes", 200), "TestRepeater") {
		t.Fatal("revoked node survived in a server cache")
	}
	if strings.Contains(request("/api/packets", 200), "abc123def4567890") {
		t.Fatal("revoked packet survived in memory")
	}
	generation.Store(0)
	request("/api/packets", 503)
}

func TestF01PublicBroadcastUsesOneCurrentEpochAndRejectsStaleCapturedRows(t *testing.T) {
	var generation atomic.Int64
	generation.Store(1)
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if generation.Load() < 1 {
			http.Error(w, "unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Generation int64 `json:"generation"`
		}{generation.Load()})
	}))
	defer authority.Close()
	db := setupTestDB(t)
	if _, err := db.conn.Exec(`CREATE TABLE public_projection_state(singleton INTEGER PRIMARY KEY,generation INTEGER,ready INTEGER);INSERT INTO public_projection_state VALUES(1,1,1)`); err != nil {
		t.Fatal(err)
	}
	hub := NewHub()
	server := NewServer(db, &Config{PublicProjection: true, PublicAuthorityURL: authority.URL, PublicAuthorityToken: "fixture"}, hub)
	server.RegisterRoutes(mux.NewRouter())
	httpServer := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer httpServer.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	message := func(hash string) WSMessage {
		return WSMessage{Type: "packet", Data: struct {
			Hash string `json:"hash"`
		}{hash}}
	}
	hub.BroadcastPublicBatch([]WSMessage{message("ready")}, 1)
	_, data, err := client.ReadMessage()
	if err != nil || !strings.Contains(string(data), "ready") {
		t.Fatalf("ready delivery: %s %v", data, err)
	}
	seen := make(chan []byte, 1)
	go func() { _, data, _ := client.ReadMessage(); seen <- data }()
	generation.Store(2)
	hub.BroadcastPublicBatch([]WSMessage{message("revoked-copy")}, 1)
	select {
	case data := <-seen:
		t.Fatalf("stale batch delivered: %s", data)
	case <-time.After(100 * time.Millisecond):
	}
	db.conn.Exec(`UPDATE public_projection_state SET generation=2,ready=1`)
	hub.BroadcastPublicBatch([]WSMessage{message("current-copy")}, 2)
	select {
	case data := <-seen:
		if !strings.Contains(string(data), "current-copy") {
			t.Fatalf("unexpected %s", data)
		}
	case <-time.After(time.Second):
		t.Fatal("current batch unavailable")
	}
	db.conn.Exec(`UPDATE public_projection_state SET ready=0`)
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err == nil || response == nil || response.StatusCode != 503 {
		t.Fatal("rebuilding projection accepted an upgrade")
	}
}

func TestF01PublicReadRechecksEpochAfterHeldHandler(t *testing.T) {
	var generation atomic.Int64
	generation.Store(1)
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Generation int64 `json:"generation"`
		}{generation.Load()})
	}))
	defer authority.Close()
	db := setupTestDB(t)
	if _, err := db.conn.Exec(`CREATE TABLE public_projection_state(singleton INTEGER PRIMARY KEY,generation INTEGER,ready INTEGER);INSERT INTO public_projection_state VALUES(1,1,1)`); err != nil {
		t.Fatal(err)
	}
	server := NewServer(db, &Config{PublicProjection: true, PublicAuthorityURL: authority.URL, PublicAuthorityToken: "fixture"}, NewHub())
	reader := newPublicProjectionReader(server)
	reader.generation, reader.routes = 1, mux.NewRouter()
	reader.routes.HandleFunc("/api/held", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("captured-private-measurement"))
		generation.Store(2)
	})
	response := httptest.NewRecorder()
	reader.middleware(http.NotFoundHandler()).ServeHTTP(response, httptest.NewRequest("GET", "/api/held", nil))
	if response.Code != 503 || strings.Contains(response.Body.String(), "captured-private") {
		t.Fatalf("held read crossed revocation: status=%d body=%s", response.Code, response.Body.String())
	}
}
