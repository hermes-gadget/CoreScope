# Journal replay review — 2026-10-23

## Status and scope

**BLOCKED: the assigned checkout does not contain the implementation named in the brief.** No replay, policy, cursor, schema, or configuration code was changed. The required baseline must be supplied as a reachable Git ref before implementing either fix; using the older Redis feature branch would omit the already-fixed fingerprint behavior.

The assigned branch is `burn/corescope-journal-20261023`, starting at `352a0ccd19896c5da6b5c821479444fdc8807cac`. Its upstream is `origin/master`, rather than the brief's `origin/main`. The local toolchain, benchmark fixtures, caches, and temporary workspace are in the untracked `.journal-work/` directory; the Makefile generates the binary under `dist/`. The pre-existing untracked `.tokensave/` directory was left alone.

## Source evidence

Commands were run from the assigned checkout:

| Command | Observed result |
| --- | --- |
| `git status --short --branch` | Assigned burn branch, tracking `origin/master`; no initial tracked modifications. |
| `git ls-remote --heads origin main master fix/projection-reload-loop` | Only `master`, at `352a0ccd19896c5da6b5c821479444fdc8807cac`. |
| `git ls-tree -r --name-only HEAD cmd/ingestor` | None of `public_projection.go`, `postgres_source.go`, or `redis_source.go` exists at HEAD. |
| `git cat-file -t 4a368c7` | Exit 128: not a valid local object. |
| `gh api repos/hermes-gadget/CoreScope/commits/4a368c7` | HTTP 422: no commit found for SHA. |
| `git log --all --oneline -- cmd/ingestor/public_projection.go cmd/ingestor/postgres_source.go cmd/ingestor/redis_source.go` | Earlier implementations exist in feature-branch history. |
| `git log -12 --oneline origin/feat/redis-live-source` | Tip `161cbf61`; this branch has the journal but predates the fingerprint fix. |
| `git grep -n -e policy_fingerprint -e corePolicyFingerprint origin/feat/redis-live-source -- cmd/ingestor` | Exit 1: no fingerprint implementation. |
| `git ls-remote --heads upstream main fix/projection-reload-loop` | No matching refs. |
| `gh api repos/hermes-gadget/CoreScope --jq '{has_issues,default_branch}'` | Issues disabled; default branch `master`. The public issue-to-PR destination also needs clarification. |

TokenSave was consulted before source exploration. It reported that the burn branch is untracked in its graph and served `master`; its results cannot establish the missing feature implementation. Shared memory locates the fingerprint fix in a deployment checkout. That checkout was not accessed under this task's envelope.

## Transaction cost experiment

This is an **isolated synthetic transaction-overhead measurement**, not a measurement of CoreScope replay, decoder work, or production storage. An additive Python 3.12.3 / SQLite 3.45.1 fixture inside `.journal-work/` inserted 8,192 records of 512 bytes each, using WAL and `synchronous=FULL` for both runs. Each database was empty at the start. The final row count was asserted. This is a single local sample, collected while the baseline verification was also running.

| Batch size | Committed transactions | Wall time |
| --- | ---: | ---: |
| 1 | 8,192 | 18.519523 s |
| 256 | 32 | 0.143741 s |

This isolates why committing every record is expensive: batching reduced commit count by 256 times and measured time by approximately 129 times in this fixture. It does **not** establish a full replay speedup or crash-safe batching in the application. At 650,000 records, the transaction counts would be 650,000 versus 2,540; those counts are an extrapolation, not an application timing result.

The experiment was run with the following Python body passed to `python3 -`:

```python
from pathlib import Path
import sqlite3, time
work = Path('.journal-work')
count = 8192
for batch in (1, 256):
    db = work / ('transaction-cost-%d.sqlite' % batch)
    con = sqlite3.connect(db)
    con.execute('PRAGMA journal_mode=WAL')
    con.execute('PRAGMA synchronous=FULL')
    con.execute('CREATE TABLE IF NOT EXISTS replay (id INTEGER PRIMARY KEY, record BLOB NOT NULL)')
    con.execute('DELETE FROM replay')
    con.commit()
    started = time.perf_counter()
    commits = 0
    for start in range(0, count, batch):
        with con:
            con.executemany('INSERT INTO replay VALUES (?, ?)',
                            ((i, b'x' * 512) for i in range(start, min(start+batch, count))))
        commits += 1
    elapsed = time.perf_counter() - started
    assert con.execute('SELECT COUNT(*) FROM replay').fetchone()[0] == count
    print('synthetic SQLite WAL FULL: records=%d batch=%d commits=%d seconds=%.6f'
          % (count, batch, commits, elapsed))
    con.close()
```

## Build and test evidence

The first `go test ./cmd/ingestor/...` attempt exited 127 because Go was absent. A local Go 1.27.3 toolchain was then downloaded from the official Go distribution and checked against the official SHA-256 `65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40`. `go version` reported `go1.27.3 linux/amd64`.

The repository has separate Go modules and no root workspace. A temporary `.journal-work/go.work` contains `go 1.27.3` and `use ../cmd/ingestor`. Test/build commands use that workspace and keep their caches and temporary files local:

```bash
journal_repo="$PWD"
export PATH="$journal_repo/.journal-work/go/bin:$PATH"
export GOWORK="$journal_repo/.journal-work/go.work"
export GOCACHE="$journal_repo/.journal-work/gocache"
export GOMODCACHE="$journal_repo/.journal-work/gomodcache"
export TMPDIR="$journal_repo/.journal-work/tmp"
export HERMES_HOME="$journal_repo/.journal-work/hermes-home"
go test ./cmd/ingestor/...
make build-ingestor
```

Both commands passed (exit 0):

```text
go test ./cmd/ingestor/...
ok  github.com/corescope/ingestor  414.402s

make build-ingestor
cd cmd/ingestor && GOOS=linux GOARCH=amd64 go build \
    -tags netgo,osusergo,sqlite_omit_load_extension -trimpath \
    -ldflags "-s -w " -o ../../dist/corescope-ingestor .
```

The build uses cgo through the Makefile. These commands validate the **unchanged master-based ingestor**, which has none of the journal paths under review. They cannot prove the requested fixes. Browser and journal-specific regression validation remain unavailable.

## Deliberately left out

- No production databases, deployment worktrees, live configuration, or secrets were accessed. No live service was started, stopped, restarted, or deployed.
- No branch replacement, speculative reconstruction of the missing fingerprint change, or merge of the earlier Redis branch was attempted.
- No browser validation was performed: this is backend investigation with no implementation available, and the task forbids service actions.
- No incremental policy deltas, metrics, durability changes, or new configuration values were added. A batch-size setting, if later introduced, should be tracked for eventual customizer exposure.

## Open questions and remaining work

1. Supply a reachable ref containing `4a368c7` and confirm the intended PR base/repository and issue destination. The missing-source question has been sent to the task owner.
2. On that baseline, benchmark the actual replay and write failing atomicity tests before implementing bounded transactional batches. A failed batch must roll back derived writes and leave its cursor unchanged; retries must preserve at-least-once delivery.
3. Implement retention-aware ledger seeking. Test the cutoff boundary, empty ledger, sequence/time ordering, widening, narrowing, and unchanged fingerprints; never rewind for a policy that has not widened.
4. Run the actual journal/fingerprint/cursor regressions, all ingestor tests, and a cgo-enabled build; document measured application before/after results separately from this microbenchmark.
5. Complete issue-to-PR flow, push the implementation, and open the PR. No implementation acceptance or readiness to merge is claimed by this report.

No issue or PR was opened while the source and destination question remained unresolved. Repository settings were not changed, and no issue was filed against the unrelated upstream baseline.
