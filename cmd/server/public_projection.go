package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// Measurement writes and epoch publication belong to the ingestor. The
// server checks current upstream authority and the local completed epoch for
// every read batch. A cache is never a source of authorization.
type publicProjectionReader struct {
	server     *Server
	client     *http.Client
	mu         sync.Mutex
	generation int64
	routes     *mux.Router
	store      *PacketStore
}

func newPublicProjectionReader(server *Server) *publicProjectionReader {
	return &publicProjectionReader{server: server, client: &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (p *publicProjectionReader) epoch(ctx context.Context) (int64, error) {
	cfg := p.server.cfg
	if !cfg.PublicProjection || cfg.PublicAuthorityURL == "" || cfg.PublicAuthorityToken == "" {
		return 0, errors.New("public projection requires current authority configuration")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.PublicAuthorityURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.PublicAuthorityToken)
	response, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, errors.New("public authority unavailable")
	}
	var authority struct {
		Generation int64 `json:"generation"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&authority); err != nil {
		return 0, err
	}
	var generation int64
	var ready int
	err = p.server.db.conn.QueryRowContext(ctx, `SELECT generation, ready FROM public_projection_state WHERE singleton=1`).Scan(&generation, &ready)
	if err != nil {
		return 0, err
	}
	if ready != 1 || generation < 1 || generation != authority.Generation {
		return 0, errors.New("public projection is stale or rebuilding")
	}
	return generation, nil
}

func (p *publicProjectionReader) routesFor(generation int64) (*mux.Router, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.routes != nil && p.generation == generation {
		return p.routes, nil
	}
	parent := p.server
	child := NewServer(parent.db, parent.cfg, parent.hub)
	child.projectionChild = true
	child.configDir, child.auth, child.knownChannels = parent.configDir, parent.auth, parent.knownChannels
	child.startedAt, child.version, child.commit, child.buildTime = parent.startedAt, parent.version, parent.commit, parent.buildTime
	var store *PacketStore
	if parent.store != nil {
		store = NewPacketStore(parent.db, parent.cfg.PacketStore, parent.cfg.CacheTTL)
		if err := store.Load(); err != nil {
			return nil, err
		}
		if !store.WaitIndexesReady(5 * time.Second) {
			return nil, errors.New("projection indexes not ready")
		}
		child.store = store
	}
	routes := mux.NewRouter()
	child.RegisterRoutes(routes)
	// Swap a whole read/cache generation. Existing requests retain their old
	// generation but must pass the final authoritative check before returning.
	p.generation, p.routes, p.store = generation, routes, store
	return routes, nil
}

const publicResponseMaxBytes = 8 * 1024 * 1024

type publicResponse struct {
	header    http.Header
	status    int
	body      bytes.Buffer
	oversized bool
}

func (r *publicResponse) Header() http.Header { return r.header }
func (r *publicResponse) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}
func (r *publicResponse) Write(value []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if r.body.Len()+len(value) > publicResponseMaxBytes {
		r.oversized = true
		return 0, errors.New("public response exceeds bounded buffer")
	}
	return r.body.Write(value)
}

func (p *publicProjectionReader) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		generation, err := p.epoch(r.Context())
		if err != nil {
			http.Error(w, "public data temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		routes, err := p.routesFor(generation)
		if err != nil {
			http.Error(w, "public data temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		response := &publicResponse{header: make(http.Header)}
		routes.ServeHTTP(response, r)
		current, err := p.epoch(r.Context())
		if err != nil || current != generation || response.oversized {
			http.Error(w, "public data temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		for key, values := range response.header {
			w.Header()[key] = values
		}
		if response.status == 0 {
			response.status = http.StatusOK
		}
		w.WriteHeader(response.status)
		_, _ = w.Write(response.body.Bytes())
	})
}
