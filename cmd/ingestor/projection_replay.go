package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ingestSQL keeps every ingest read, prepared statement and derived write on
// the same transaction during replay. Maintenance still owns the base pool.
type ingestSQL interface {
	Exec(string, ...interface{}) (sql.Result, error)
	QueryRow(string, ...interface{}) *sql.Row
	Prepare(string) (*sql.Stmt, error)
}

const publicProjectionReplayBatchSize = 256

// withProjectionReplayTx lends a transaction-scoped Store to the existing
// decoder. The original Store and its prepared statements are never swapped,
// so maintenance cannot accidentally join the replay transaction. writerMu
// covers one bounded page; the scoped Store does not acquire it recursively.
func (s *Store) withProjectionReplayTx(ctx context.Context, apply func(*Store) error) (err error) {
	waitStart := time.Now()
	writerMu.Lock()
	wait := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		writerMu.Unlock()
		recordWriterTiming("public_projection_replay", wait, time.Since(holdStart), "replay page")
	}()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var synchronous int
	if err := conn.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		return err
	}
	// Only reconstructible derived data uses NORMAL. The private journal and
	// ready=0 fence were committed with FULL before entering this path. The
	// final ready=1 publication also uses FULL and syncs the completed WAL.
	defer func() {
		_, restoreErr := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA synchronous=%d", synchronous))
		if restoreErr != nil {
			// Never return a connection with reduced durability to live ingest.
			_ = conn.Raw(func(interface{}) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("restore replay durability: %w", restoreErr))
		}
	}()
	if _, err := conn.ExecContext(ctx, `PRAGMA synchronous=NORMAL`); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	batch := &Store{db: s.db, ingestDB: tx, replayTx: tx, path: s.path,
		sampleIntervalSec: s.sampleIntervalSec, linkedCompanions: s.linkedCompanions,
		relayTouched: s.relayTouched}
	committed := false
	defer func() {
		if !committed {
			s.relayTouched = nil // includes cancellation and panic after relay writes
		}
	}()
	batch.prefixIdx.store(s.prefixIdx.load())
	batch.neighborGraph.store(s.neighborGraph.load())
	batch.advertEvidenceComplete.Store(s.advertEvidenceComplete.Load())
	if err := batch.prepareStatements(); err != nil {
		return err
	}
	if err := apply(batch); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	s.relayTouched = batch.relayTouched
	for _, counters := range []struct{ dst, src *atomic.Int64 }{
		{&s.Stats.TransmissionsInserted, &batch.Stats.TransmissionsInserted},
		{&s.Stats.ObservationsInserted, &batch.Stats.ObservationsInserted},
		{&s.Stats.DuplicateTransmissions, &batch.Stats.DuplicateTransmissions},
		{&s.Stats.NodeUpserts, &batch.Stats.NodeUpserts},
		{&s.Stats.ObserverUpserts, &batch.Stats.ObserverUpserts},
		{&s.Stats.WriteErrors, &batch.Stats.WriteErrors},
		{&s.Stats.SignatureDrops, &batch.Stats.SignatureDrops},
		{&s.Stats.ClientUnlinkedDropped, &batch.Stats.ClientUnlinkedDropped},
		{&s.Stats.RelayTouches, &batch.Stats.RelayTouches},
	} {
		counters.dst.Add(counters.src.Load())
	}
	s.Stats.WALCommits.Add(1)
	return nil
}
