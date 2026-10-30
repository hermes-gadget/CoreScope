package main

import (
	"context"
	"fmt"
	"time"
)

// Paired historical readers cannot advance their cursors while the old
// composite uniqueness rule might still collapse distinct upstream IDs.
func waitForUpstreamIdentityReady(ctx context.Context, store *Store) error {
	for {
		var oldIndex, newIndexes int
		err := store.db.QueryRow(`SELECT
			COUNT(*) FILTER (WHERE name = 'idx_observations_dedup'),
			COUNT(*) FILTER (WHERE name IN ('idx_observations_dedup_legacy', 'idx_observations_upstream_id'))
			FROM sqlite_master WHERE type='index'`).Scan(&oldIndex, &newIndexes)
		if err != nil {
			return err
		}
		if oldIndex == 0 && newIndexes == 2 {
			return nil
		}
		status, statusErr := store.AsyncMigrationStatus("upstream_observation_identity_v1")
		if statusErr == nil && (status == "failed" || status == "cancelled") {
			return fmt.Errorf("upstream identity migration %s; source cursor held", status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
