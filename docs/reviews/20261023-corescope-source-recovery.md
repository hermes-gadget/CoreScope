# Projection-reload source recovery — 2026-10-23

The morning hunt recovered usable source, but **the original deployment commit is still absent from this clone and has no published branch on the GitHub fork**. The remaining handoff is to publish `fix/projection-reload-loop` from the deployment checkout, expected at `4a368c75be9564ae55ce0c7f7b3e4326ee2e42a0`. Do not repeat the earlier assumption that all implementation source is unavailable.

Evidence: the [completed hunt report](https://github.com/hermes-gadget/CoreScope/blob/7753e18d39b0b38e02f7c680fa48846c2de519b9/docs/reviews/20261023-corescope-hunt.md) supersedes the [initial blocked journal review](https://github.com/hermes-gadget/CoreScope/blob/7753e18d39b0b38e02f7c680fa48846c2de519b9/docs/reviews/20261023-corescope-journal.md). Remote refs, local object absence, scratch files and hashes were checked again for this note. VPS observations below are from the morning hunt; no new VPS access was needed.

## Where to look, in priority order

| Location | What is there / what remains uncertain |
| --- | --- |
| **VPS: `$HOME/corescope-ukmesh/src`**, branch `fix/projection-reload-loop` | Best source for the original Git history. The morning read-only `git ls-remote "$HOME/corescope-ukmesh/src" refs/heads/fix/projection-reload-loop` returned the full `4a368c75…` SHA above. `ls -l` confirmed the four ingestor files below, with sizes matching local scratch. The complete commit/tree was never fetched or compared. |
| **Local: `$HOME/.hermes/cache/scratch/ccfix/`** | Recovered ingestor `public_projection.go` (14,443 bytes), `postgres_source.go` (22,215), `redis_source.go` (19,077), and `public_projection_noop_bump_test.go` (1,867). These preserve the policy-fingerprint/no-op-generation behavior absent from the older feature branch. They are source copies, not the original commit object. |
| **Local: `$HOME/.hermes/cache/scratch/`** | Server `public_projection.go` (8,330 bytes) and `public_projection_regression_test.go` (1,988). The regression covers keeping a generation's routes cached when background indexes are not yet ready, preventing repeated cold loads. Both files match recovery commit `851aa218` byte-for-byte. |
| **GitHub/local: `burn/corescope-hunt-20261023`**, tip `7753e18d39b0b38e02f7c680fa48846c2de519b9` | Usable reconstruction is already published. Recovery commit `851aa218fb5e1af88a0bcb679db146ea6e2ac16f` combines `feat/redis-live-source` (`161cbf6191503b808bad80758f90f02f04b86f36`) with preserved ingestor/server files. Follow-on `b7bfcaf7eb1069a684f8cae63f037b9c309725ed` adds replay batching and retention-aware cursor seeking. Neither is proof of exact deployment ancestry. |
| **Local: `$HOME/ukmesh-review-20261011/CoreScope-1`** | Older source only; `4a368c75…` is absent here too. Not a replacement for the VPS ref. |
| Backups, session archives, Docker image layers | Fallback candidates only. No matching archive member/ref or local CoreScope image was found within the search coverage below. Unlisted/incomplete archives and images on other hosts remain unverified. |

Scratch SHA-256 values, rechecked unchanged:

| Path relative to `$HOME/.hermes/cache/scratch/` | SHA-256 |
| --- | --- |
| `ccfix/public_projection.go` | `901e2b14d3e4e8d3e04ec2e25472a618b36fd1e925b7b84f723df5f035111d54` |
| `ccfix/postgres_source.go` | `abe928deb602241fb0f9fb70d6c29339683c353a56be91a40003b002fe988ec2` |
| `ccfix/redis_source.go` | `1665c59c46cb83f3fa7b5c9f51ba00b7fb4949fa98449035268f56c2461d5918` |
| `ccfix/public_projection_noop_bump_test.go` | `526c86c33e17f0df0212536ba2f5ba51eb0f5c9c01d0c50ac62cfed646df90c1` |
| `public_projection.go` | `bc338386fd909cc55afdd63f09bf75fa4572fcee9294b74554762bdec678c767` |
| `public_projection_regression_test.go` | `2915d9a25e75a554af4435bf08395ef2b68c37654dfa2afaa9b11df29237057c` |

## What was searched

| Search | Result and limits |
| --- | --- |
| Local refs/history: `git log --all` with `projection\|journal\|redis\|postgres`; `git rev-list --all --objects`; `git reflog --all`; `git cat-file -t 4a368c7` | Older feature history found; requested commit absent. The full-SHA `cat-file` check still exits 128 in both local clones. |
| Object recovery: `git fsck --full --no-reflogs --unreachable`; `git fsck --full --lost-found` | Unreachable blobs/trees and two dangling trees; no requested commit recovered. Lost-found pass exited 0. |
| GitHub refs: `git ls-remote --heads origin main master fix/projection-reload-loop`, and equivalent upstream lookup | Morning lookup found only fork `master` among the requested refs. Recheck also confirms the published hunt branch, but no projection-reload branch on either remote. Fork master remains `352a0ccd19896c5da6b5c821479444fdc8807cac`; upstream master is `7fa86fe1c1304b88e0f2a44052465dfb86abb8a6`. |
| GitHub commit lookup: `gh api repos/hermes-gadget/CoreScope/commits/4a368c7` | Initial journal investigation received HTTP 422, no commit found. This note rechecked remote refs rather than repeating that API request. |
| Source-name search: `find "$HOME" -type f \( -name public_projection.go -o -name postgres_source.go -o -name redis_source.go \) -print` | Found scratch copies and older review-clone files, plus the recovered checkout files after restoration. Focused scratch inventory and hashes were rechecked for this note. |
| Home-wide Git-directory and archive inventory; focused CoreScope names in backups, deployment backups, remote staging, downloads, scratch, review notes and review checkout | No matching named archive. A permission error in another lane's PostgreSQL fixture limits coverage; it was not bypassed. |
| Archive member/ref search: `rg --files --hidden --no-ignore` inventory, then `python3 .hunt-work/search-archives.py` | 400 candidates, **385 complete listings, 15 incomplete, zero matches**. Used `tar -tf`, ZIP metadata and `git bundle list-heads`; no member contents extracted. Two large session archives hit the 20-second bound; other incomplete entries were invalid archives/test fixtures or non-Git bundles. Dependencies, installed caches/toolchains and secret directories were pruned. Not an exhaustive archive-content search. |
| Docker: local image inventory and `docker image inspect corescope-fork:pg2145-b7` | Initial inventory had 26 images and no CoreScope tag; target inspect exited 1. No image available for `docker save`/layer extraction. No production container inspected or run. |
| VPS: read-only SSH `git ls-remote` and `ls -l` in the deployment checkout | Located the original branch and matching ingestor file sizes. No remote fetch/push, database query, configuration access, service action or write occurred during the hunt. |

The local hunt evidence remains in the original checkout's untracked `.hunt-work/`: `archive-search-summary.json`, `archive-search.jsonl`, `source-archive-inventory.err`, `search-archives.py` and `fsck-lost-found.log`. The summary still reads 400/385/15 with no matches. TokenSave was consulted first but served the older `master` graph for the untracked hunt branch; it cannot establish source absence. Shared memory was consulted, but Git objects, files and the hunt report provide the evidence here.

## Shortest command to supply the original source

**Run on the VPS that holds the deployment checkout:**

```bash
git -C "$HOME/corescope-ukmesh/src" push https://github.com/hermes-gadget/CoreScope.git fix/projection-reload-loop
```

This publishes the existing branch and its reachable history to the known fork without depending on that checkout's `origin` setting. It needs the operator's existing GitHub write access. The expected branch tip is `4a368c75be9564ae55ce0c7f7b3e4326ee2e42a0`; if it has moved, ensure that original commit is included too. A different tip alone does not establish that the original is reachable. No force push, checkout, service restart or database/configuration change is needed. This command is an operator handoff and was **not executed** by this documentation sprint.

If GitHub push access is unavailable, create a self-contained bundle on that VPS and supply the resulting file:

```bash
git -C "$HOME/corescope-ukmesh/src" bundle create /tmp/corescope-projection-reload.bundle fix/projection-reload-loop
```

After the branch is supplied, the receiving clone can fetch it without changing its checkout:

```bash
git fetch origin fix/projection-reload-loop
git rev-parse FETCH_HEAD
git diff 4a368c75be9564ae55ce0c7f7b3e4326ee2e42a0 851aa218fb5e1af88a0bcb679db146ea6e2ac16f
```

The SHA check and full-tree comparison close the provenance gap. The hunt's tests establish the reconstruction's behavior; they do not establish byte-identical recovery of the deployment tree. This sprint adds documentation only, with no runtime, configuration or customizer implications.
