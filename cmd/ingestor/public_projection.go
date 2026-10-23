package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

type publicProjectionRecord struct {
	ObservationID     string     `json:"observationId"`
	Topic             string     `json:"topic"`
	Payload           []byte     `json:"payload"`
	Network           string     `json:"network"`
	RXNodeID          string     `json:"rxNodeId"`
	SrcNodeID         string     `json:"srcNodeId"`
	PathHashes        []string   `json:"pathHashes"`
	PathHashSizeBytes int        `json:"pathHashSizeBytes"`
	Tag               string     `json:"tag"`
	Source            MQTTSource `json:"source"`
}

var publicObservationID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func enrichPublicProjectionRecord(incoming, previous publicProjectionRecord) (publicProjectionRecord, error) {
	var next, old pgEnvelope
	if err := json.Unmarshal(incoming.Payload, &next); err != nil {
		return incoming, err
	}
	if err := json.Unmarshal(previous.Payload, &old); err != nil {
		return incoming, err
	}
	nextTime, nextErr := time.Parse(time.RFC3339Nano, next.Timestamp)
	oldTime, oldErr := time.Parse(time.RFC3339Nano, old.Timestamp)
	if !strings.EqualFold(next.Raw, old.Raw) || nextErr != nil || oldErr != nil || !nextTime.Equal(oldTime) {
		return incoming, errors.New("upstream observation identity payload changed")
	}
	// A less enriched duplicate must retain every participant used by later
	// revocation. Conflicting nonempty identity fields fail the batch closed.
	for _, pair := range []struct {
		next *string
		old  string
	}{
		{&incoming.Topic, previous.Topic}, {&incoming.Network, previous.Network},
		{&incoming.RXNodeID, previous.RXNodeID}, {&incoming.SrcNodeID, previous.SrcNodeID},
	} {
		if *pair.next == "" {
			*pair.next = pair.old
		} else if pair.old != "" && !strings.EqualFold(*pair.next, pair.old) {
			return incoming, errors.New("upstream observation participant changed")
		}
	}
	if len(incoming.PathHashes) == 0 {
		incoming.PathHashes = previous.PathHashes
		incoming.PathHashSizeBytes = previous.PathHashSizeBytes
	} else if len(previous.PathHashes) > 0 {
		if incoming.PathHashSizeBytes != previous.PathHashSizeBytes || len(incoming.PathHashes) != len(previous.PathHashes) {
			return incoming, errors.New("upstream observation path changed")
		}
		for i, path := range incoming.PathHashes {
			if !strings.EqualFold(path, previous.PathHashes[i]) {
				return incoming, errors.New("upstream observation path changed")
			}
		}
	}
	next.Raw, next.Timestamp = old.Raw, old.Timestamp
	if next.SNR == nil {
		next.SNR = old.SNR
	}
	if next.RSSI == nil {
		next.RSSI = old.RSSI
	}
	if next.Origin == "" {
		next.Origin = old.Origin
	}
	if next.Direction == "" {
		next.Direction = old.Direction
	}
	var err error
	incoming.Payload, err = json.Marshal(next)
	return incoming, err
}

func (r publicProjectionRecord) participant() redisPendingEntry {
	return redisPendingEntry{network: r.Network, rxNodeID: r.RXNodeID, srcNodeID: r.SrcNodeID,
		pathHashes: r.PathHashes, pathHashSizeBytes: r.PathHashSizeBytes}
}

// The journal is private ingestor-owned replay state. Only a completed,
// current generation is public. Rebuild never rewrites a mixed-source DB:
// initialization requires an empty DB or an existing complete journal.
type corePublicProjection struct {
	mu            sync.Mutex
	store         *Store
	loadPolicy    func(context.Context) (*corePublicPolicy, error)
	apply         func(publicProjectionRecord) error
	retentionDays int
	fingerprint   string // privacy-rule digest behind the last completed refresh
}

func newCorePublicProjection(store *Store, load func(context.Context) (*corePublicPolicy, error),
	apply func(publicProjectionRecord) error, retentionDays int) (*corePublicProjection, error) {
	if retentionDays < 1 {
		return nil, errors.New("public projection requires positive packet retention")
	}
	store.backfillWg.Wait()
	var existing int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='public_projection_state'`).Scan(&existing); err != nil {
		return nil, err
	}
	if existing == 0 {
		for _, table := range publicMeasurementTables {
			var installed int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&installed); err != nil {
				return nil, err
			}
			if installed == 0 {
				continue
			}
			var populated bool
			predicate := ""
			if table == "scope_match_totals" {
				predicate = " WHERE unique_matches<>0 OR explicit_over_derived<>0 OR ambiguous<>0 OR none_matches<>0"
			}
			if err := store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM ` + table + predicate + ` LIMIT 1)`).Scan(&populated); err != nil {
				return nil, err
			}
			if populated {
				return nil, fmt.Errorf("public projection requires an empty dedicated DB; %s lacks revocable provenance", table)
			}
		}
	}
	_, err := store.db.Exec(`CREATE TABLE IF NOT EXISTS public_projection_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), generation INTEGER NOT NULL, ready INTEGER NOT NULL,
  policy_fingerprint TEXT NOT NULL DEFAULT '');
  INSERT OR IGNORE INTO public_projection_state(singleton,generation,ready) VALUES(1,0,0);
  CREATE TABLE IF NOT EXISTS public_projection_journal (
   id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id TEXT NOT NULL UNIQUE,
   observed_at TEXT NOT NULL, record BLOB NOT NULL);
  CREATE INDEX IF NOT EXISTS public_projection_journal_time ON public_projection_journal(observed_at)`)
	if err != nil {
		return nil, err
	}
	// Databases created before the fingerprint column existed need it added.
	// SQLite has no ADD COLUMN IF NOT EXISTS, so the duplicate-column error is
	// the expected steady-state outcome.
	if _, err := store.db.Exec(`ALTER TABLE public_projection_state ADD COLUMN policy_fingerprint TEXT NOT NULL DEFAULT ''`); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		return nil, err
	}
	// Do NOT force ready=0 here. Both ensure() and Batch() clear ready before
	// any derived write and restore it afterwards, so ready=1 in the database
	// means the last writer completed and the tables match the journal. The
	// old unconditional reset replayed the whole journal (~1h at production
	// size) on every restart for no correctness gain.
	return &corePublicProjection{store: store, loadPolicy: load, apply: apply, retentionDays: retentionDays}, nil
}

func (p *corePublicProjection) Refresh(ctx context.Context) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	policy, err := p.loadPolicy(ctx)
	if err != nil {
		return 0, err
	}
	if err := p.ensure(policy); err != nil {
		return 0, err
	}
	return policy.generation, nil
}

var publicMeasurementTables = []string{
	"advert_route_evidence", "neighbor_edges", "client_rf_samples", "client_rx_observations",
	"client_receptions", "client_observers", "node_declared_regions", "observer_metrics", "dropped_packets",
	"observations", "transmissions", "scope_match_totals", "inactive_nodes", "nodes", "observers",
}

func (p *corePublicProjection) ensure(policy *corePublicPolicy) error {
	if policy == nil || policy.generation < 1 {
		return errors.New("public policy unavailable")
	}
	p.fingerprint = policy.fingerprint
	var generation int64
	var ready int
	var fingerprint string
	if err := p.store.db.QueryRow(`SELECT generation,ready,COALESCE(policy_fingerprint,'') FROM public_projection_state WHERE singleton=1`).Scan(&generation, &ready, &fingerprint); err != nil {
		return err
	}
	switch decideProjectionAction(generation, ready, fingerprint, policy) {
	case projectionReady:
		if fingerprint == "" {
			// Baseline the digest on the first start of this build so the next
			// counter-only bump can be recognised as a no-op.
			_, err := p.store.db.Exec(`UPDATE public_projection_state SET policy_fingerprint=? WHERE singleton=1`, policy.fingerprint)
			return err
		}
		return nil
	case projectionAdoptGeneration:
		// Only the counter moved: the visibility rules are identical, so the
		// derived tables are still correct. Adopt the epoch without replaying.
		_, err := p.store.db.Exec(`UPDATE public_projection_state SET generation=?,policy_fingerprint=? WHERE singleton=1`,
			policy.generation, policy.fingerprint)
		return err
	}
	if _, err := p.store.db.Exec(`UPDATE public_projection_state SET ready=0 WHERE singleton=1`); err != nil {
		return err
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -p.retentionDays).Format(time.RFC3339Nano)
	if err := p.store.WriterTx("public_projection", func(tx *sql.Tx) error {
		for _, table := range publicMeasurementTables {
			var exists int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&exists); err != nil {
				return err
			}
			if exists > 0 {
				if _, err := tx.Exec(`DELETE FROM ` + table); err != nil {
					return fmt.Errorf("rebuild %s: %w", table, err)
				}
			}
		}
		_, err := tx.Exec(`DELETE FROM public_projection_journal WHERE observed_at < ?`, cutoff)
		return err
	}); err != nil {
		return err
	}
	p.store.prefixIdx.store(prefixIndex{})
	p.store.neighborGraph.store(&NeighborGraph{})
	p.store.relayTouched = nil
	scopeMatchCounters.unique.Store(0)
	scopeMatchCounters.explicitOverDerived.Store(0)
	scopeMatchCounters.ambiguous.Store(0)
	scopeMatchCounters.none.Store(0)
	scopeMatchCounters.sinceUnix.Store(time.Now().Unix())
	// Page the private journal rather than retaining all history in memory.
	var cursor int64
	for {
		rows, err := p.store.db.Query(`SELECT id,record FROM public_projection_journal WHERE id>? ORDER BY id LIMIT 256`, cursor)
		if err != nil {
			return err
		}
		type saved struct {
			id     int64
			record publicProjectionRecord
		}
		batch := make([]saved, 0, 256)
		for rows.Next() {
			var item saved
			var raw []byte
			if err := rows.Scan(&item.id, &raw); err != nil {
				rows.Close()
				return err
			}
			if err := json.Unmarshal(raw, &item.record); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, item := range batch {
			if policy.allows(item.record.participant()) {
				if err := p.apply(item.record); err != nil {
					return err
				}
			} else if _, err := p.store.db.Exec(`DELETE FROM public_projection_journal WHERE id=?`, item.id); err != nil {
				return err
			}
			cursor = item.id
		}
	}
	if err := p.store.backfillAdvertEvidence(context.Background(), p.store.db); err != nil {
		return err
	}
	_, err := p.store.db.Exec(`UPDATE public_projection_state SET generation=?,ready=1,policy_fingerprint=? WHERE singleton=1`,
		policy.generation, policy.fingerprint)
	return err
}

// projectionDecision is the outcome of comparing the stored projection state
// with the current privacy policy.
type projectionDecision int

const (
	projectionReady           projectionDecision = iota // stored tables already match the policy
	projectionAdoptGeneration                           // rules unchanged; only the counter moved
	projectionRebuild                                   // rules changed (or prior rebuild incomplete)
)

// decideProjectionAction is pure so the no-op-bump behaviour is unit-testable.
func decideProjectionAction(generation int64, ready int, fingerprint string, policy *corePublicPolicy) projectionDecision {
	switch {
	case generation == policy.generation && ready == 1:
		return projectionReady
	case generation == policy.generation:
		return projectionRebuild
	case ready == 1 && fingerprint != "" && fingerprint == policy.fingerprint:
		return projectionAdoptGeneration
	default:
		return projectionRebuild
	}
}

// Fingerprint returns the privacy-rule digest behind the last completed
// refresh. The pg source uses it to avoid resetting its ledger cursor when a
// generation bump did not change the rules.
func (p *corePublicProjection) Fingerprint() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fingerprint
}

func (p *corePublicProjection) Batch(ctx context.Context, records []publicProjectionRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	policy, err := p.loadPolicy(ctx)
	if err != nil {
		return err
	}
	if err := p.ensure(policy); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	if len(records) > 2000 {
		return errors.New("public source batch exceeds its bound")
	}
	if _, err := p.store.db.Exec(`UPDATE public_projection_state SET ready=0 WHERE singleton=1`); err != nil {
		return err
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -p.retentionDays)
	allowed := make([]publicProjectionRecord, 0, len(records))
	for _, record := range records {
		if !publicObservationID.MatchString(record.ObservationID) || record.ObservationID == pgZeroObsID {
			return errors.New("public projection requires stable upstream identity")
		}
		if !policy.allows(record.participant()) {
			continue
		}
		var envelope pgEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return err
		}
		observed, err := time.Parse(time.RFC3339Nano, envelope.Timestamp)
		if err != nil {
			return err
		}
		if observed.Before(cutoff) {
			continue
		}
		var previousJSON []byte
		err = p.store.db.QueryRow(`SELECT record FROM public_projection_journal WHERE observation_id=?`, record.ObservationID).Scan(&previousJSON)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			var previous publicProjectionRecord
			if err := json.Unmarshal(previousJSON, &previous); err != nil {
				return err
			}
			record, err = enrichPublicProjectionRecord(record, previous)
			if err != nil {
				return err
			}
		}
		if !policy.allows(record.participant()) {
			continue
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := p.store.db.Exec(`INSERT INTO public_projection_journal(observation_id,observed_at,record)
   VALUES(?,?,?) ON CONFLICT(observation_id) DO UPDATE SET record=excluded.record`, record.ObservationID, envelope.Timestamp, raw); err != nil {
			return err
		}
		allowed = append(allowed, record)
	}
	for _, record := range allowed {
		if err := p.apply(record); err != nil {
			return err
		}
	}
	_, err = p.store.db.Exec(`UPDATE public_projection_state SET generation=?,ready=1,policy_fingerprint=? WHERE singleton=1`,
		policy.generation, policy.fingerprint)
	return err
}
