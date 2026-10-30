# UKMesh public sources

UKMesh public ingestion requires a dedicated revocable SQLite projection.
The ingestor owns all measurement and journal writes; the server opens the
database read-only. Public source configuration is rejected without
`publicProjection: true`, exactly one public PostgreSQL source, positive
`retention.packetDays`, and no MQTT sources.

Use a new empty database. Existing copied measurements without the projection
journal cannot be reconstructed safely; initialization refuses to purge them.
Preserve the old database and backfill a new database from UKMesh's committed
ledger. Migrations 058 and 059, including the historical coverage fence, must
be installed upstream. The ledger follows commit order, including older late
commits, and replays retained history after cursor loss or a policy change.
History already deleted upstream cannot be recovered.

Example ingestor configuration (replace all placeholders outside source
control):

```json
{
  "dbPath": "./ukmesh-public.db",
  "publicProjection": true,
  "mqttSources": [],
  "retention": { "packetDays": 30 },
  "postgresSources": [{
    "name": "ukmesh-completeness",
    "dsn": "<read-only UKMesh PostgreSQL DSN>",
    "batchSize": 500,
    "requirePublicVisibility": true,
    "maxEnvelopePastDays": -1
  }],
  "redisSources": [{
    "name": "ukmesh-public-live",
    "publicURL": "https://<private-backend>/internal/ingest/public/entries",
    "publicToken": "<MQTT_RAM_PUBLIC_READ_TOKEN>",
    "requirePublicVisibility": true,
    "maxEnvelopePastDays": -1
  }]
}
```

`redisSources` is optional. Its public mode uses the filtered HTTP source and
receives no raw Redis credential; `url` and `privacyDsn` must be absent.
The endpoint requires a bearer token configured upstream with at least 32
characters. It returns at most 500 currently authorized observations per poll.
Its cursor skips private observations without copying raw private payloads.
PostgreSQL remains the completeness path. Keep the SQLite journal and backup
files private to the ingestor/operator and grant the server only read access.

Add these settings to the server configuration, pointing at the same upstream
authority used by the ingestor:

```json
{
  "dbPath": "./ukmesh-public.db",
  "publicProjection": true,
  "publicAuthorityURL": "https://<private-backend>/internal/ingest/public/policy",
  "publicAuthorityToken": "<MQTT_RAM_PUBLIC_READ_TOKEN>"
}
```

The ingestor keeps stable identity and original participant provenance in a
private replay journal. A generation transition fences readiness, removes
revoked journal records and reconstructs measurement tables, observer counts
and advert evidence from the remaining records. It pages journal replay in
groups of 256. Startup replays the journal to repair partial writes after a
crash. Both sources save cursors only after the tracked batch completes;
duplicate observations enrich existing fields without incrementing counts.

Every public API request checks current authority and local readiness before
reading and again before returning a buffered response. Buffers are bounded
at 8 MiB; larger responses fail closed. Caches and packet indexes are replaced
on generation changes. Public WebSocket upgrades require a current completed
projection; live batches recheck authority once before fanout, are bounded to
1,000 frames/8 MiB and have a one-second write deadline. Slow sockets close.
Data already delivered and bytes already in flight cannot be recalled.

Unavailable authority, upstream rematerialization, rebuilding, failed SQLite
writes or a stale local generation produce temporary public unavailability.
Old databases with upstream IDs are also fenced when authority is unconfigured.
Public mode intentionally pauses derived background builders that could write
a previous snapshot after reconstruction. Validate public completeness and
performance on an isolated rig before enabling this experimental mode.

For an explicitly private deployment, sources may set
`requirePublicVisibility: false` and the server may set
`privateUpstreamDeployment: true`. Those settings permit raw private data and
must not be used for a public mirror.
