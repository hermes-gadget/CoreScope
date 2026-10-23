# CoreScope source recovery and replay/cursor fixes — 2026-10-23

## Result

The source was found. The three ingestor files and the fingerprint regression test survived in local build scratch, and a read-only VPS lookup confirmed branch `fix/projection-reload-loop` at `4a368c75be9564ae55ce0c7f7b3e4326ee2e42a0`. The recovered implementation builds, and both requested changes are implemented and tested on `burn/corescope-hunt-20261023`.

Recovery commit: `851aa218fb5e1af88a0bcb679db146ea6e2ac16f`.
Replay/cursor implementation and tests: `b7bfcaf7eb1069a684f8cae63f037b9c309725ed`.

The recovery commit merges the already available `origin/feat/redis-live-source` (`161cbf6191503b808bad80758f90f02f04b86f36`) and overlays the local scratch files, including the server reload-loop source and its regression test. Review the new replay/cursor change relative to the recovery commit; the larger difference from `master` includes the previously missing source dependency. The original journal investigation report remains in history and in this tree.

## Search evidence and provenance

Home paths below use `$HOME`; host addresses, credentials, live configuration, and production data are omitted.

| Search / command | Evidence |
| --- | --- |
| `git status --short --branch`; `git worktree list --porcelain` | Assigned hunt branch, initially at `1cedc960a7a16d868f30e604d550b2eb31e6fa97`; one registered worktree. Existing `.journal-work/` and `.tokensave/` were untracked. |
| `git log --all --format='%h %s' --regexp-ignore-case --grep='projection\|journal\|redis\|postgres'`; `git rev-list --all --objects`; `git reflog --all` | Older PostgreSQL/Redis/projection implementations exist, but the requested commit was not a local object. |
| `git cat-file -t 4a368c7` | Exit 128, invalid object. The second local CoreScope clone under `$HOME/ukmesh-review-20261011/CoreScope-1` also lacks this object and has older source files. |
| `git fsck --full --no-reflogs --unreachable`; `git fsck --full --lost-found` | Unreachable blobs/trees and two dangling trees; no missing commit recovered. The lost-found pass exited 0. |
| `git ls-remote --heads origin main master fix/projection-reload-loop` | Only `master`, `352a0ccd19896c5da6b5c821479444fdc8807cac`. The equivalent upstream lookup also lacked the named branch. |
| `find "$HOME" -type f \( -name public_projection.go -o -name postgres_source.go -o -name redis_source.go \) -print` | Found the usable copies in `$HOME/.hermes/cache/scratch/ccfix/`, the server copy in scratch, and older files in the review clone. It also saw the newly recovered copies in this checkout. |
| Home-wide Git-directory and archive inventory (`*.tar`, `*.tar.gz`, `*.tgz`, `*.tar.xz`, `*.tar.zst`, `*.zip`, `*.bundle`, `*.pack`, `*.tar.bz2`, `*.7z`) | Completed with a permission error in another lane's PostgreSQL fixture. No access restriction was bypassed. Focused CoreScope archive-name searches in backups, deployment backups, remote staging, downloads, scratch, review notes, and the review checkout found no matching archive. |
| `rg --files --hidden --no-ignore` archive inventory, then `python3 .hunt-work/search-archives.py` | Searched member names in 400 candidate archives using `tar -tf`, Python ZIP metadata, and `git bundle list-heads`: no target filename/ref matches. There were 385 complete listings and 15 incomplete ones (two large session archives hit the 20-second bound; the rest were invalid archives/test fixtures or non-Git bundles). Dependency trees, installed caches/toolchains, and secret directories were pruned for this second pass. No member contents were extracted; archive coverage is not claimed exhaustive. |
| `docker image inspect corescope-fork:pg2145-b7 --format '{{.Id}} {{json .RepoTags}} {{json .RootFS.Layers}}'` | Exit 1: no such local image. The initial local inventory contained 26 images and no CoreScope tag, so `docker save` / layer extraction was not applicable. No production container was run or inspected. |
| SSH command: `git ls-remote "$HOME/corescope-ukmesh/src" refs/heads/fix/projection-reload-loop` | VPS returned `4a368c75be9564ae55ce0c7f7b3e4326ee2e42a0`. |
| SSH command: `ls -l` for the four named ingestor source/test files in that checkout | VPS sizes were 14,443 / 22,215 / 19,077 / 1,867 bytes for projection / PostgreSQL / Redis / no-op test, matching the local scratch copies. |

Both SSH invocations used `BatchMode=yes`, `ConnectTimeout=10`, `StrictHostKeyChecking=yes`, `UpdateHostKeys=no`, `ControlMaster=no`, `ControlPath=none`, `RemoteCommand=none`, and `ForwardAgent=no`. They executed only `git ls-remote` and `ls`. No VPS writes, fetches, Docker operations, database queries, service actions, or configuration access occurred.

Original scratch SHA-256 values were checked again after implementation; they remained unchanged:

| File in `ccfix/` | SHA-256 |
| --- | --- |
| `public_projection.go` | `901e2b14d3e4e8d3e04ec2e25472a618b36fd1e925b7b84f723df5f035111d54` |
| `postgres_source.go` | `abe928deb602241fb0f9fb70d6c29339683c353a56be91a40003b002fe988ec2` |
| `redis_source.go` | `1665c59c46cb83f3fa7b5c9f51ba00b7fb4949fa98449035268f56c2461d5918` |
| `public_projection_noop_bump_test.go` | `526c86c33e17f0df0212536ba2f5ba51eb0f5c9c01d0c50ac62cfed646df90c1` |

TokenSave was consulted first but served the older `master` graph because the hunt branch was untracked; it reported 42 tokens saved. Shared OpenViking memory supplied the deployment checkout path. These are discovery aids: the local file contents, Git results, builds, and tests provide the implementation evidence. The complete VPS commit payload was not fetched, so this is a tested reconstruction from available source, not a claim that the entire recovered tree is byte-identical to the VPS tree.

## Implementation and correctness

Replay now applies at most 256 journal records in one transaction. The decoder receives a transaction-scoped `Store`, whose prepared statements, identity probes, observer/node writes, companion writes, and rejected-record journal deletions share that transaction. The live store and its prepared statements are never swapped. The existing writer mutex covers one page, with no recursive acquisition by the scoped store, and maintenance can proceed between pages.

Only replay pages use `synchronous=NORMAL` on a pinned connection. The previous setting is restored before the connection returns to live ingest, including error, cancellation, and panic paths; a failed restoration discards the connection. The journal and initial `ready=0` fence use the existing FULL path. Final `ready=1` publication follows restoration to FULL. This design uses SQLite's documented WAL durability behavior: NORMAL transactions survive an application crash but may be lost on power failure; FULL commits synchronize the WAL. The readiness fence makes incomplete reconstruction retryable. See [SQLite synchronous semantics](https://www.sqlite.org/pragma.html#pragma_synchronous).

The journal cursor advances only after the page commits. A failed page rolls back derived writes and private-journal deletions together and leaves `ready=0`. Restart rebuilds from the journal. Completed restarts and unchanged policy fingerprints retain the b7 behavior and avoid replay. Existing authority-lookup timeouts were retained; a long rebuild was not placed under a new ten-second overall deadline.

On a genuine fingerprint change, the PostgreSQL source resolves `MIN(seq) - 1` over ledger entries at or after the packet-retention cutoff, with the source's network scope. It uses the ledger head when there is no retained match. This preserves the exact cutoff boundary, gaps, and observation timestamps that arrive out of sequence. Taking the maximum expired sequence would skip eligible observations. An unsuccessful seek leaves the prior cursor and policy metadata unchanged. Fresh/legacy state adoption and no-op generation bumps retain their existing behavior; successful source batches still use the existing durable-completion barrier before saving their cursor.

Replay is linear in retained journal records with a 256-record page in memory and one transaction per page. The retention seek is a single aggregate on a genuine rule change, never a per-record query; its worst case can scan the ledger once. No source-side index or live migration was added. The upstream ledger schema inspected locally has `(network, seq)` indexing; production query timing was not measured.

## Verification

Toolchain: existing worktree-local Go 1.27.3, `CGO_ENABLED=1`. Commands used `GOWORK=off`, worktree-local module/build caches, and a temporary `HERMES_HOME`. No live Hermes configuration was read or changed. `.hunt-work/run` records these environment settings; logs and disposable browser artifacts remain untracked in `.hunt-work/`.

The following commands passed (exit 0). PostgreSQL/Redis integration runs used isolated local fixtures on loopback ports 25438 and 26394, with no production mounts or data.

| Exact test/build command after environment setup | Result |
| --- | --- |
| `(cd cmd/ingestor && go test ./... -count=1)` on recovered baseline | PASS, 323.262 s |
| `(cd cmd/server && go test ./... -count=1)` on recovered server source | PASS, 113.369 s; server source did not change afterward |
| `(cd cmd/ingestor && go test ./... -count=1)` with local PostgreSQL/Redis fixtures | PASS, 242.734 s |
| `(cd internal/dbschema && go test ./... -count=1)` | PASS, 0.564 s |
| `make build-ingestor build-server` | PASS on recovered baseline and final implementation, using the Makefile's cgo build path |
| `(cd cmd/ingestor && go test -race -run '^TestProjectionReplay\|^TestPgPolicyCursor\|^TestPgRetention\|^TestF01CopiedDerivedProjectionRevokesAndReplaysAfterFailure$\|^TestF08RealPairedSourcesCrashFailureAndRestartBothOrders$' -count=1 -timeout=120s)` | PASS, 6.008 s |
| `(cd cmd/ingestor && go test -run 'TestProjectionReplay\|TestPgPolicyCursor\|TestPgRetention\|TestF0[178]\|TestCorePolicyFingerprint\|TestDecideProjectionAction' -count=1)` | PASS, 3.587 s after consolidating the shared test decoder callback |
| `git diff --check` and staged-diff review | PASS |

New regression evidence includes:

- Before batching, injected failure after writes in page two left **257 observations instead of 256**; the test failed against recovered code.
- After batching, only the complete first page survives; page-two observation writes and its private-journal deletion roll back. Retry produces exact observation and observer totals, and 515 journal records use three replay transactions.
- Killing a real child process inside uncommitted page two preserves the first 256 observations, `ready=0`, and the existing PostgreSQL cursor file. Reopening the crashed database and replaying restores all 515 records.
- Cancellation and panic roll back scoped writes and restore FULL durability; nested instrumented writer helpers do not deadlock.
- Real PostgreSQL checks cover an empty ledger, exact cutoff equality, out-of-order timestamps, absent networks, and all-expired history. Unit checks cover widened/narrowed fingerprints, unchanged generations/rules, legacy/fresh state, and failed seeks. Existing paired PostgreSQL/Redis crash tests passed in both delivery orders.

### Actual replay measurement

Command on recovered code and after batching:

```bash
cd cmd/ingestor
go test -run '^$' -bench '^BenchmarkProjectionReplay$' -benchtime=1x -count=3
```

The benchmark seeds 2,048 journal observations with unique upstream IDs and one repeated encrypted packet frame, then runs the actual decoder, derived writes, and readiness publication on an empty dedicated SQLite/WAL fixture. Setup is outside the timed section. Both runs used the same local toolchain and filesystem. Final comparison samples were taken with this lane's full test suites idle; unrelated host activity was not controlled. The baseline rerun used a local `git archive` of recovery commit `851aa218`, with the same benchmark body adapted only to the earlier decoder callback signature.

| Implementation | Samples (seconds) | Median |
| --- | --- | ---: |
| Recovered autocommit replay | 24.253609757, 22.845139513, 18.560681209 | 22.845139513 s |
| Bounded transactional replay | 0.353782544, 0.341710729, 0.354130136 | 0.353782544 s |

The final median improvement is about 65 times in this duplicate-heavy 2K-observation fixture. Earlier concurrent-test samples were 18.864 / 67.325 / 21.531 s before and 0.665 / 0.643 / 0.890 s after (about 32 times by median); those samples are retained in the local logs and were not mixed into the final comparison. This is application-path evidence, not the earlier synthetic INSERT experiment. It is not a production outage estimate or a benchmark of the full deployed journal or a diverse 30K-packet dataset.

### Browser validation

The connected Browser skill was read and attempted; its runtime rejected this environment with `Browser use requires a trusted Node REPL browser service`. The requested legacy `openclaw` browser profile was unavailable here. The already installed Playwright Chromium was used for an actual local browser check instead; no dependency was added.

A temporary Go test generated an isolated public-projection database using the new real replay path. A loopback authority fixture advertised generation 1. The final server binary served that database on port 23581. `node .hunt-work/browser-smoke.cjs` passed: CoreScope's packets page rendered the synthetic packet after selecting **All time**, `/api/stats` and `/api/packets?limit=5` returned 200, and reload followed by reselecting All time rendered it again with zero page errors. The screenshot `.hunt-work/browser-smoke.png` was visually inspected. This is a small-fixture UI smoke check; backend and crash tests establish replay/cursor correctness. The temporary fixture-generation test was removed afterward. The local server, authority process, and two task-labelled PostgreSQL/Redis fixture containers were stopped after validation; production services were untouched.

## Deliberately left out and remaining work

- No deployment, production restart, production database operation, live migration, source-original modification, secret access, or live configuration change. The branch and report remain local for review; no PR, public audit, or external message was published.
- No speculative reconstruction was used while source was missing. Once found, the existing feature history and preserved files supplied the baseline. Full VPS commit/tree comparison remains an operator handoff if exact deployment ancestry is required before merging.
- Live source batching, incremental policy deltas, ledger schema changes, and production-scale tuning are outside these two fixes. Source retention already uses `retention.packetDays`; no configuration key was added. The fixed replay page size of 256 should be exposed through configuration/customizer in a later milestone only if operational tuning is needed.
- Review the recovery commit separately from the replay/cursor commit, then merge or deploy through the owner's normal process. Production timing and service readiness after deployment remain unverified.

PROOF: replay/cursor implementation and regression tests are committed at `b7bfcaf7eb1069a684f8cae63f037b9c309725ed`; this handoff is committed separately on the same branch, whose final SHA is supplied in the completion summary.
