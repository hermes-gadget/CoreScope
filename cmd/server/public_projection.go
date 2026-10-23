package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	// lastFailureUnix rate-limits build/epoch failure logging so a stale
	// authority cannot flood the log at request rate.
	lastFailureUnix atomic.Int64
}

// projectionIndexWait bounds the best-effort wait for the background index
// builds before the route table is installed. It is NOT a readiness gate:
// handlers already answer 503 + Retry-After while SubpathIndexReady() /
// PathHopIndexReady() are false (see index_ready_1008.go).
var projectionIndexWait = 2 * time.Second

// awaitProjectionIndexes is a test seam for the index-readiness wait.
var awaitProjectionIndexes = func(store *PacketStore, timeout time.Duration) bool {
	return store.WaitIndexesReady(timeout)
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

func (p *publicProjectionReader) currentGeneration() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.routes == nil {
		return 0
	}
	return p.generation
}

// logFailure emits at most one line per minute so a persistently failing
// authority/build is visible without flooding the log at request rate.
func (p *publicProjectionReader) logFailure(format string, args ...any) {
	now := time.Now().Unix()
	last := p.lastFailureUnix.Load()
	if now-last < 60 {
		return
	}
	if p.lastFailureUnix.CompareAndSwap(last, now) {
		log.Printf(format, args...)
	}
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
		started := time.Now()
		if err := store.Load(); err != nil {
			return nil, err
		}
		// Index builds are deliberately NOT a gate here. At ukmesh scale
		// the subpath build takes ~7s for 700k paths; a fixed readiness
		// budget shorter than that used to make routesFor return an error
		// after Load() had already succeeded. The freshly loaded store was
		// then discarded, so every following request cold-loaded the whole
		// 1.3GB DB again — a self-sustaining reload loop that served no
		// traffic and pinned every core. Handlers gate the few endpoints
		// that need these indexes and answer 503 + Retry-After until they
		// land, which is the documented production contract.
		if !awaitProjectionIndexes(store, projectionIndexWait) {
			log.Printf("[public-projection] indexes still building after %s — serving with per-handler 503 gates until ready", projectionIndexWait)
		}
		log.Printf("[public-projection] store loaded for generation %d in %s", generation, time.Since(started).Round(time.Millisecond))
		child.store = store
	}
	routes := mux.NewRouter()
	child.RegisterRoutes(routes)
	// Swap a whole read/cache generation. Existing requests retain their old
	// generation but must pass the final authoritative check before returning.
	p.generation, p.routes, p.store = generation, routes, store
	return routes, nil
}

// watchAuthority keeps the projection routes warm. It polls the authority
// epoch at a low rate and builds a newly published generation in the
// background, so the first visitor after a restart or a privacy generation
// change does not pay the cold-load cost and an idle instance does not stay
// cold forever. Failures are retried on the next tick; the request path is
// unaffected.
func (p *publicProjectionReader) watchAuthority(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		generation, err := p.epoch(ctx)
		cancel()
		if err == nil && generation != p.currentGeneration() {
			started := time.Now()
			if _, buildErr := p.routesFor(generation); buildErr != nil {
				p.logFailure("[public-projection] background route build failed (generation=%d): %v", generation, buildErr)
			} else {
				log.Printf("[public-projection] routes rebuilt for generation %d in %s", generation, time.Since(started).Round(time.Millisecond))
			}
		}
		time.Sleep(interval)
	}
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
			p.logFailure("[public-projection] epoch unavailable: %v", err)
			http.Error(w, "public data temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		routes, err := p.routesFor(generation)
		if err != nil {
			p.logFailure("[public-projection] route build failed (generation=%d): %v", generation, err)
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
