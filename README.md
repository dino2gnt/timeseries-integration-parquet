# OpenNMS Time Series Storage plugin — Apache Parquet

An OpenNMS [Time Series Storage](https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html)
(TSS) plugin that stores samples as [Apache Parquet](https://parquet.apache.org/) files on
the local filesystem. It is pure-JVM (no native libraries): parquet-java with
`LocalInputFile`/`LocalOutputFile` and a `PlainParquetConfiguration`, so it needs no Hadoop
runtime even though it link-depends on a few Hadoop classes.

See [`DESIGN.md`](DESIGN.md) for the full design and rationale.

## How it works

* **Sharding** — the `resourceId` intrinsic tag is already a slash-delimited resource path, so
  it is used directly as a relative directory under `<baseDir>/data/` (each path element is
  percent-encoded). Samples are further split into time partitions `p=<value>` (daily by
  default).
* **Files** — samples are buffered in memory per partition and flushed to a new immutable
  `part-<uuid>.parquet` on a row threshold, a timer, or shutdown (schema: `time` INT64
  epoch-micros, `name` UTF8, `value` DOUBLE). Append = add a file, so writes never rewrite
  existing data. Each file is written to a hidden temp and **atomically moved** into place, so
  readers and compaction only ever see complete files — never a half-written or 0-byte one.
  Buffered-but-unflushed samples are readable only after a flush (a small staleness window) and
  are lost on an unclean crash.
* **Compaction** — frequent flushing keeps memory and the crash window small but yields one file
  per collection cycle per shard (hundreds of tiny files/shard/day, which Parquet reads poorly).
  A periodic background pass merges each partition's small files into one larger file and, for
  shards where a metric was deleted, drops the deleted metric's rows (the physical purge). This is
  the usual columnar *flush-small / compact-later* split; tuning `flush.interval` alone can't fix
  small files because collection is bursty (one burst per period). Compaction is resilient to a
  bad file: a 0-byte part file (a crash leftover from before atomic writes) is removed, an
  unreadable/corrupt one is skipped and left in place with a warning, and either way the rest of
  the pass proceeds instead of aborting. Run a pass on demand with the `tss-parquet:compact`
  shell command (below).
* **Catalog** — an in-memory index of the full metric definitions backs `findMetrics` (regex/
  equality over intrinsic + meta tags) and resolves the complete metric for reads.
* **Retention** — a scheduled sweep deletes whole partition directories older than the
  retention window and prunes catalog entries with no partitions left.
* **Aggregation** — never pushed down; OpenNMS aggregates the raw samples in memory.

## Project layout

| Module           | Purpose                                                                       |
|------------------|-------------------------------------------------------------------------------|
| `plugin`         | `ParquetStorage` + `PathMapper`, `MetricCatalog`, `SampleWriter/Reader`, `RetentionManager`, blueprint. Built as a **fat bundle** that embeds the whole parquet/hadoop stack (see Packaging) |
| `karaf-features` | The Karaf `features.xml` (just the fat plugin bundle + feature deps)          |
| `assembly/kar`   | Packages everything into a deployable, self-contained `.kar`                  |

## Configuration

ConfigAdmin PID **`org.opennms.timeseries.parquet`** (e.g.
`$OPENNMS_HOME/etc/org.opennms.timeseries.parquet.cfg`):

| Property                   | Default        | Meaning                                              |
|----------------------------|----------------|------------------------------------------------------|
| `baseDir`                  | `${rrd.base.dir}` | Data root; falls back to the `rrd.base.dir` system property when blank |
| `partition.duration`       | `P1D`          | Time-partition granularity (ISO-8601 duration)       |
| `retention`                | `P365D`        | Delete data older than this                          |
| `retention.sweep.interval` | `P1D`          | How often the retention sweep runs                   |
| `compression`              | `UNCOMPRESSED` | Parquet codec: `UNCOMPRESSED` or `LZ4_RAW` (see note below) |
| `catalog.pruneOnExpiry`    | `true`         | Prune catalog entries whose partitions all expired   |
| `flush.maxRows`            | `8192`         | Buffered rows per partition that trigger an eager flush |
| `flush.interval`           | `PT60S`        | Cadence of the background buffer flush               |
| `compaction.enabled`       | `true`         | Merge small partition files and purge deleted rows   |
| `compaction.interval`      | `PT1H`         | Cadence of the background compaction                 |

> **Compression:** `UNCOMPRESSED` and `LZ4_RAW` are supported and tested. `LZ4_RAW` uses a
> pure-Java LZ4 (`aircompressor`) wired in through a custom `CompressionCodecFactory` that
> bypasses parquet's Hadoop-`Configuration` codec plumbing (which would otherwise drag in
> woodstox and the rest of the excluded Hadoop transitive tree). Any other codec —
> `SNAPPY`, `GZIP`, `ZSTD`, deprecated `LZ4`, etc. — is rejected with an
> `IllegalArgumentException`: they would require that Hadoop codec path (and `SNAPPY`/`ZSTD`
> are native besides). Leave it at `UNCOMPRESSED` or set `LZ4_RAW`.

## Build

* Requires JDK 17–21 and Maven 3.6+. Build targets Java 17 bytecode.
* `mvn install` — runs unit + contract tests and statically verifies the Karaf feature resolves
  (`karaf:verify`). For a fully offline build, add `-Dkaraf.verify.skip=true`.
* Deployable archive: `./assembly/kar/target/opennms-timeseries-parquet-plugin-opa-v2.0.0.kar`
  (self-contained: the single fat plugin bundle, which itself embeds the parquet/hadoop/re2j/etc jars).

## Packaging (fat bundle)

The plugin is a **fat OSGi bundle**: the whole third-party stack is embedded inside it via bnd
`Embed-Dependency`, rather than shipped as one wrapped bundle per jar. Embedded set (12 jars):
`re2j`, `aircompressor`, `jts-core`, `jrobin` (for `import-from-rrd`),
`parquet-{hadoop,column,encoding,format-structures,common,jackson}`,
`hadoop-{common,mapreduce-client-core}`. Only `org.opennms.integration.api.*`, `org.slf4j` and the
karaf shell action API are mandatory imports; every reference into the excluded parts of the hadoop/parquet tree
(commons-configuration2, guava, woodstox, `snappy-java`, `zstd-jni`, ...) is an optional import,
since the pure-JVM UNCOMPRESSED/LZ4_RAW + local-file path never loads them.

This is deliberate. The `org.apache.parquet` package is **split** across `parquet-common`,
`parquet-column` and `parquet-hadoop`; shipping those as separate bundles makes OSGi wire that one
package to a single exporter, so e.g. `parquet-column` can't see `parquet-common`'s `Preconditions`
→ `NoClassDefFoundError` at runtime (which `karaf:verify` does **not** catch). Embedding puts all
parquet classes on one classloader and dissolves the split. Two related gotchas the fat bundle also
settles: no `Karaf-Commands` header (it would make Karaf scan/`defineClass` every embedded class and
choke on the excluded refs + multi-release-jar entries), and `jts-core` must be embedded because
parquet 1.17's write path references `org.locationtech.jts` unconditionally. See `DESIGN.md` §10.

> **Verifying the shipped set:** a green `mvn test` does not prove the bundle ships enough — the
> Maven test classpath still contains excluded transitives. Extract the jars embedded in the built
> bundle and run the runtime-path tests against only those + slf4j + integration-api. That models
> the OSGi classpath (and is what caught the missing `jts-core`).

## Install into OpenNMS

1. Enable the integration TSS layer:
   `echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timeseries.properties`
2. Copy the `.kar` from `./assembly/kar/target` to `$OPENNMS_HOME/deploy`.
3. In the Karaf shell (`ssh -p 8101 admin@localhost`):
   `feature:install opennms-timeseries-parquet-plugin`

## Shell commands

* `tss-parquet:compact` — runs a compaction pass immediately instead of waiting for the scheduled
  `compaction.interval` timer (merges small part files, removes empty ones, and purges deleted
  metrics' rows). It runs on the store's single maintenance thread, so it never races the scheduled
  sweep/compaction, and prints how many partitions were modified. Useful after a bulk delete or to
  force cleanup while validating a deployment.

* `tss-parquet:import-from-rrd -s <rrd|jrb> --yes-really` — backfills the store from local RRDtool
  (`.rrd`) or JRobin (`.jrb`) files under `rrd.base.dir`. Options: `-s/--storage-tool`
  (`rrdtool|rrd|jrobin|jrb`, required), `-t/--threads` (default: CPUs), `-v/--verbose`,
  `--yes-really` (required confirmation). It walks the `response` and `snmp` trees (honoring
  `org.opennms.rrd.storeByGroup`) and reconstructs samples from the finest RRAs. It honors
  `org.opennms.rrd.storeByForeignSource`: when **true**, store-by-id paths (`snmp/<nodeId>/…`) are
  rewritten to store-by-foreign-source `resourceId`s (`snmp/fs/<fs>/<fid>/…`) via the OpenNMS node
  table; when **false**, the on-disk path is used verbatim so imported `resourceId`s match what live
  collection writes. (The node-table lookup is skipped entirely in the false case.)

  **How it writes:** unlike live collection, an import knows each partition's full sample set up
  front, so it uses a **direct bulk-write path** (`bulkImport`) that groups samples by
  `(shard,partition)` and writes each partition as **one** file — bypassing the async buffer. For a
  greenfield backfill that is the optimal layout with **no compaction needed**; if a partition
  already holds live data the import adds one file that the scheduled compaction (or
  `tss-parquet:compact`) later merges.

  **Requirements:** `rrdtool` mode shells out to the `rrdtool` binary (set the `rrd.binary` system
  property) and parses its `dump` XML via JAXB; `jrobin` mode reads `.jrb` via the embedded jrobin
  library. The command also needs the OpenNMS `javax.sql.DataSource` service (for the node →
  foreign-source mapping). It is an admin/one-shot tool — run it while collection is quiet.

## Karaf runtime status

Confirmed working on a live OpenNMS **Horizon 37** instance: the feature installs, the plugin
bundle reaches **Active**, the `ParquetStorage` blueprint bean instantiates, and the plugin
persists data. (`karaf:verify` only proves the feature *resolves*; the runtime issues that had to
be fixed — split package, the `Karaf-Commands` scan, the missing `jts-core` — are exactly the class
of problem static resolution can't see. See `DESIGN.md` §10.)

Checklist to re-run when validating a new build or a new OpenNMS version:

1. `feature:install` the feature and confirm the plugin bundle reaches **Active** (`bundle:list`).
2. Confirm the `ParquetStorage` service is registered (`service:list TimeSeriesStorage`) and the
   blueprint container is created (`bundle:services`, no `GracePeriod`).
3. Let OpenNMS collect data, then verify `part-*.parquet` files appear under `<baseDir>/data/…`
   **and that graphs render** (exercises the read + `findMetrics` path end-to-end).
4. Watch the log through at least one `compaction.interval`/`retention.sweep.interval` so the
   background maintenance passes run against real on-disk data. Any `NoClassDefFoundError` there
   means a genuinely-needed jar is missing from the embedded set (re-run the shipped-set check
   above); it is no longer a split-package/wiring symptom.

## Links
* Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
* OpenNMS Integration API: https://github.com/OpenNMS/opennms-integration-api
