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
  existing data. Buffered-but-unflushed samples are readable only after a flush (a small
  staleness window) and are lost on an unclean crash.
* **Compaction** — frequent flushing keeps memory and the crash window small but yields one file
  per collection cycle per shard (hundreds of tiny files/shard/day, which Parquet reads poorly).
  A periodic background pass merges each partition's small files into one larger file and, for
  shards where a metric was deleted, drops the deleted metric's rows (the physical purge). This is
  the usual columnar *flush-small / compact-later* split; tuning `flush.interval` alone can't fix
  small files because collection is bursty (one burst per period).
* **Catalog** — an in-memory index of the full metric definitions backs `findMetrics` (regex/
  equality over intrinsic + meta tags) and resolves the complete metric for reads.
* **Retention** — a scheduled sweep deletes whole partition directories older than the
  retention window and prunes catalog entries with no partitions left.
* **Aggregation** — never pushed down; OpenNMS aggregates the raw samples in memory.

## Project layout

| Module           | Purpose                                                                       |
|------------------|-------------------------------------------------------------------------------|
| `plugin`         | `ParquetStorage` + `PathMapper`, `MetricCatalog`, `SampleWriter/Reader`, `RetentionManager`, blueprint |
| `karaf-features` | The Karaf `features.xml` (plugin bundle + wrapped parquet/hadoop/re2j bundles) |
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
  (self-contained: embeds the wrapped parquet/hadoop/re2j jars).

## Install into OpenNMS

1. Enable the integration TSS layer:
   `echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timeseries.properties`
2. Copy the `.kar` from `./assembly/kar/target` to `$OPENNMS_HOME/deploy`.
3. In the Karaf shell (`ssh -p 8101 admin@localhost`):
   `feature:install opennms-plugins-timeseries-parquet-plugin`

## Karaf smoke test — still required

The build proves the feature **resolves** (OSGi wiring: the plugin bundle's parquet/re2j imports
are satisfied by the wrapped bundles, and Hadoop's heavy transitives are marked optional because
the local-file path never exercises them). It does **not** prove runtime behaviour. Before
trusting this in production, smoke-test on a real OpenNMS/Karaf:

1. `feature:install` the feature and confirm every bundle reaches **Active** (`bundle:list`),
   especially the wrapped `hadoop-common`, `hadoop-mapreduce-client-core` and `parquet-*`.
2. Confirm the `ParquetStorage` service is registered (`service:list TimeSeriesStorage`) and the
   blueprint container is created (`bundle:services`, no `GracePeriod`).
3. Let OpenNMS collect data, then verify `part-*.parquet` files appear under `<baseDir>/data/…`
   and that graphs render (read path works end-to-end inside Karaf).
4. Watch the log for `NoClassDefFoundError`/`ClassNotFoundException` (an over-aggressive optional
   import) and for split-package mis-wiring across the parquet bundles — the two risks static
   resolution can't rule out. If a `ServiceLoader`/TCCL failure appears, add Aries SPI-Fly
   (not in Karaf's standard features; must be bundled).

## Links
* Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
* OpenNMS Integration API: https://github.com/OpenNMS/opennms-integration-api