# Parquet-backed OpenNMS TSS plugin — design

Status: **implemented** (M1–M6 complete; pending a live-OpenNMS smoke test — see README).
Engine decision: **parquet-java (pure-JVM)**.

This plugin implements `org.opennms.integration.api.v1.timeseries.TimeSeriesStorage`
(API 2.0.0) with Apache Parquet files on the local filesystem as the backing store,
sharded storeByForeignSource-style, with configurable time partitioning and a
plugin-managed data-retention lifecycle.

## 1. Foundational facts (verified against OpenNMS core `develop` + the API sources)

- **`resourceId` is the shard path.** OpenNMS builds it as `String.join("/", resourcePath.elements())`
  (`TimeseriesUtils.toResourceId`). It is **slash-delimited** and already contains the
  storeByForeignSource tree:

  | OpenNMS setting | example `resourceId` |
  |---|---|
  | default (store by nodeId) | `snmp/1/eth0/mib2-X-interfaces` |
  | `org.opennms.rrd.storeByForeignSource=true` | `snmp/fs/Servers/node1/eth0/mib2-X-interfaces` |

  The **last element is the group** (RRD-file equivalent). The datasource is the separate
  `name` intrinsic tag (e.g. `ifHCInOctets`). `fs/<foreignSource>/<foreignId>` only appear
  in intrinsic tags when SBFS is enabled; when off, only the numeric nodeId is present.
  → Sharding is free: **use `resourceId` directly as a relative directory path.** Branch on
  whether path element 2 is `fs` or numeric if we need to distinguish layouts.

- **A Metric is:** intrinsic `{resourceId, name}` + meta `mtype` + operator-configured meta
  tags (searchable) + external tags (collected strings, **not** searchable).
  The contract test (`AbstractStorageIntegrationTest`) asserts `findMetrics` round-trips
  **all three tag categories exactly** → the catalog must persist the full metric.

- **Search** = regex (RE2) over `resourceId` + equality/regex over `name` and meta tags.
  There are **no `_idx` tags** in the integration-API path (that's legacy Newts).

- **Persistence may be asynchronous** — the contract test calls `waitForPersistingChanges()`
  after `store`, so buffering/batching is allowed (and Parquet, being columnar/immutable,
  requires it).

- `getTimeseries` is queried by the metric's **intrinsic key only** (`Metric.getKey()` =
  sorted intrinsic tags). `delete` must make a metric vanish from both `findMetrics` and
  `getTimeseries`; other metrics unaffected.

## 2. Engine: parquet-java, pure-JVM

- `org.apache.parquet:parquet-hadoop:1.17.1` (pulls `parquet-column`/`parquet-common` with
  `LocalInputFile`/`LocalOutputFile`, added in 1.14.0).
- Writer: **example/Group** (`ExampleParquetWriter` + `GroupWriteSupport`) — lightest, no
  Avro/Jackson tail (Jackson from Avro is the main Karaf conflict source). Accept the
  "example only" Javadoc note; revisit parquet-avro only if needed.
- Use `builder(OutputFile)` + a `PlainParquetConfiguration` so we don't need a real Hadoop
  `Configuration` instance. (parquet still *link*-requires `hadoop-common` — see §9.)
- Codec: **UNCOMPRESSED** and **LZ4_RAW** are implemented (pure-Java via `aircompressor`).
  Rather than the Hadoop codec path — parquet instantiates codecs through a Hadoop
  `Configuration`, and even `new Configuration(false)` in hadoop-common 3.3.6 needs woodstox at
  class-init, cascading into the §9/§10 transitive tree — we supply a custom
  `AircompressorCodecFactory` via `withCodecFactory(...)`, which handles LZ4_RAW directly
  (byte-compatible with parquet's own `Lz4RawCodec`) and passes UNCOMPRESSED through. Any other
  codec (SNAPPY/ZSTD — native; GZIP/deprecated LZ4 — would need the bypassed Hadoop path) throws
  `IllegalArgumentException` from the factory.
- Consequence: `supportsAggregation` returns **NONE only**; OpenNMS aggregates in memory.
  Reads are hand-rolled range scans (with row-group predicate pushdown by time as an opt).

## 3. Components

```
ParquetStorage implements TimeSeriesStorage
 ├─ PathMapper       resourceId -> sharded dir; timestamp -> time-partition dir
 ├─ MetricCatalog    full Metric <-> key; tag-matcher search (findMetrics)
 ├─ SampleWriter     buffer samples per (shard,partition); roll parquet files
 ├─ SampleReader     resolve series via catalog; read partition files in range
 └─ RetentionManager scheduled sweep: delete expired partitions + prune catalog
```

## 4. On-disk layout

```
<baseDir>/
  data/  snmp/fs/Servers/node1/eth0/mib2-X-interfaces/   # = sanitized resourceId
           p=2026-07-06/                                   # time partition (daily default)
             part-<seq>.parquet                            # rolled, append-only files
  catalog/
     metrics.parquet                                       # durable full-metric catalog
```

- **Data schema:** `time INT64` (epoch **micros**, preserves the Instant precision the test
  uses), `name BINARY(UTF8)`, `value DOUBLE`. `resourceId` is implicit in the path.
- **Partition dir** `p=<value>`, value = timestamp truncated to the configured unit
  (`p=2026-07-06` daily, `p=2026-07-06-14` hourly). Self-describing so retention can parse
  the cutoff from the dir name alone.
- **Sanitization:** percent-encode path-unsafe chars per `resourceId` element, keep `/` as
  separator. Canonical `resourceId` lives in the catalog, so paths never need decoding.
  (The contract test's `resourceId` is colon-delimited with no `/` → one shard element;
  fine, only round-trip correctness is asserted, not layout.)

## 5. Write path (`store`)
1. Group samples by `(shardDir, partition)`.
2. Append to a per-bucket in-memory buffer; upsert each distinct metric into the catalog.
3. Flush a bucket to a **new** `part-<seq>.parquet` on size/row threshold or flush-interval
   (unique names → concurrent flushes never clobber; append = add-a-file). Background
   flusher drains partial buffers on a timer and on shutdown.

## 6. Read path
- **`findMetrics(matchers)`** — evaluated against the in-memory catalog: EQUALS / NOT_EQUALS
  / EQUALS_REGEX / NOT_EQUALS_REGEX applied across intrinsic + meta tags (external excluded).
  Returns fully-reconstructed metrics. NPE on null, IAE on empty (per contract).
- **`getTimeseries(request)`** — look up the key in the catalog (absent → empty, which also
  covers logical deletes); compute shardDir; enumerate `p=*` dirs overlapping `[start,end]`;
  read those parquet files filtering `name == metric.name AND time in [start,end]`; sort.

## 7. Delete (`delete(metric)`)
Partition files hold many `name`s (whole group), so physical delete = file rewrite.
Plan: **logical delete** — drop the key from the catalog (both `findMetrics` and
`getTimeseries` then report it gone); physically purge rows during the next
compaction/retention pass. Delete is rare, so this is a good tradeoff.

## 8. Lifecycle / retention (what Parquet can't do itself)
`RetentionManager` on a `ScheduledExecutorService` (configurable cadence): walk the data
tree, parse each `p=<value>` dir, `rm -rf` whole partition dirs older than the retention
window (default **365 days**), then prune catalog entries whose series retain no partitions.
Whole-directory deletion is cheap and safe (self-contained files).

## 9. Configuration (Karaf ConfigAdmin PID `org.opennms.timeseries.parquet`, blueprint `cm:`)
`baseDir`, `partition.duration` (default `P1D`), `retention` (default `P365D`),
`retention.sweep.interval`, `flush.maxRows`, `flush.interval`, `compression`,
`catalog.pruneOnExpiry`.

- **`baseDir` default = the `rrd.base.dir` system property** (the existing OpenNMS RRD/JRB
  data root, e.g. `$OPENNMS_HOME/share/rrd`). Do **not** invent a new default path that may
  not exist. Resolve `System.getProperty("rrd.base.dir")` at init; the parquet `data/` and
  `catalog/` trees live under it, mirroring where RRD/Newts data already lands. Fail fast in
  `init()` if the property is unset and no explicit `baseDir` is configured.

## 10. Packaging risk (biggest unknown) — de-risk early
parquet-java still **link-requires `hadoop-common`** even with local files, and Hadoop jars
ship no OSGi manifests. The OpenNMS Cortex TSS plugin's proven pattern: thin plugin bundle,
one `mvn:` bundle per dependency in `features.xml`, `wrap:` non-OSGi jars, and a shade module
only for colliding libs — **not** an Embed-Dependency uber-bundle. Likely also need Aries
**SPI-Fly** for ServiceLoader/TCCL. **Milestone 0.5: a Karaf spike** — deploy a trivial
bundle that news up a `ParquetWriter(LocalOutputFile)` and writes a temp file inside a
running Karaf, to prove the dependency set resolves before building the whole plugin.

## 11. Testing
- Unit-test PathMapper (resourceId→dir, both layouts + sanitization) and MetricCatalog
  (all four matcher types).
- Enable `AbstractStorageIntegrationTest`: `@Before` points `createStorage()` at a temp dir,
  `waitForPersistingChanges()` forces a flush.
- IT for retention sweep and logical delete.

## 12. Milestones
1. ✅ Rename `skel`→`parquet` / `Skel`→`Parquet`; build green.
2. ✅ PathMapper + in-memory catalog + `findMetrics` (unit-tested).
3. ✅ Parquet write+read; contract test enabled and passing.
4. ✅ Retention sweep + logical delete. (Row-level physical purge of a logically-deleted
   metric's rows within a shared group file is deferred — logical deletes age out via retention.)
5. ✅ Config wiring (ConfigAdmin PID `org.opennms.timeseries.parquet`).
6. ✅ Karaf packaging: thin plugin bundle + one wrapped bundle per non-OSGi dependency
   (parquet ×6, hadoop ×2, re2j), Hadoop imports marked optional. `karaf:verify` resolves the
   feature; a self-contained `.kar` is produced. ⏳ Live-OpenNMS smoke test still pending (README).

Post-plan, done:
- Durable catalog sidecar (`catalog/metrics.parquet`, loaded on `init()`, flushed on metric-set
  change and on shutdown) — metrics (incl. meta/external tags, which live only in the catalog)
  survive a restart.
- Async flush buffer (§5): samples accumulate per `(shard,partition)` and flush on the
  `flush.maxRows` threshold, the `flush.interval` timer, or shutdown; `flush()` forces a drain.
- Compaction: a periodic pass merges each partition's small files (one per flush/cycle) into one
  larger file, and for shards where a metric was deleted drops the dead rows — this is also the
  §7 row-level physical purge of logical deletes. Retention + compaction share one maintenance
  thread so they never race. Config: `compaction.enabled`, `compaction.interval`.

Deferred / future: buffer-aware reads (a buffered sample
is only readable after its flush); tiered compaction (the current pass rewrites the whole
partition each run — O(partition size) write amplification; for high volume prefer hourly
`partition.duration` or a longer `compaction.interval`).