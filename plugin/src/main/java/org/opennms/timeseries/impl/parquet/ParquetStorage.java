/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2026 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2026 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.timeseries.impl.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableDataPoint;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Parquet-backed implementation of the OpenNMS {@link TimeSeriesStorage} interface.
 *
 * <p>Samples are sharded by {@code resourceId} and time-partitioned on the local filesystem
 * (see {@link PathMapper}); each {@code store} flushes a batch to immutable {@code part-*.parquet}
 * files (see {@link SampleWriter}). Metric definitions are held in an in-memory {@link MetricCatalog}
 * that backs {@code findMetrics} and resolves the full metric (all tag categories) for reads.
 * Aggregation is never pushed down &mdash; OpenNMS aggregates the raw samples in memory.</p>
 *
 * <p>Milestone 3 scope: the write and read paths and the storage contract. Durable catalog
 * persistence (rebuild on restart), retention, logical-delete physical purge and configuration
 * wiring arrive in later milestones.</p>
 */
public class ParquetStorage implements TimeSeriesStorage, ParquetMaintenance {

    private static final Logger log = LoggerFactory.getLogger(ParquetStorage.class);

    /** System property holding the OpenNMS RRD data root; the default base directory (DESIGN §9). */
    static final String RRD_BASE_DIR_PROPERTY = "rrd.base.dir";

    /** Default time-partition granularity (configurable in a later milestone). */
    static final Duration DEFAULT_PARTITION_DURATION = Duration.ofDays(1);

    /** Default retention window: data older than this is swept away. */
    static final Duration DEFAULT_RETENTION = Duration.ofDays(365);

    /** Default cadence of the background retention sweep. */
    static final Duration DEFAULT_RETENTION_SWEEP_INTERVAL = Duration.ofDays(1);

    /** Default sample-file compression codec. */
    static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.UNCOMPRESSED;

    /** Default per-partition row threshold that triggers an eager buffer flush. */
    static final int DEFAULT_FLUSH_MAX_ROWS = 8192;

    /**
     * Default cadence of the background buffer flush. Kept short so the crash window and in-memory
     * buffer stay small; the resulting small files are consolidated by the compactor rather than by
     * a long flush interval.
     */
    static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(60);

    /** Default cadence of the background small-file compaction. */
    static final Duration DEFAULT_COMPACTION_INTERVAL = Duration.ofHours(1);

    /** A partition is merged once it accumulates at least this many part files. */
    static final int COMPACTION_MIN_FILES = 2;

    /** In-memory index of the full metric definitions; the authority for {@link #findMetrics}. */
    private final MetricCatalog catalog = new MetricCatalog();
    private final SampleReader sampleReader = new SampleReader();
    private final MetricCatalogStore catalogStore = new MetricCatalogStore();
    /** Serializes sidecar rewrites so concurrent store/delete calls never clobber the file. */
    private final Object catalogFlushLock = new Object();

    // Configuration. Populated by the typed constructors (tests) or the string setters that the
    // blueprint fills from the ConfigAdmin PID; consumed in init(). Defaults match DESIGN §9.
    private Path baseDir;
    private Duration partitionDuration = DEFAULT_PARTITION_DURATION;
    private Duration retention = DEFAULT_RETENTION;
    private Duration retentionSweepInterval = DEFAULT_RETENTION_SWEEP_INTERVAL;
    private CompressionCodecName compression = DEFAULT_COMPRESSION;
    private boolean pruneCatalogOnExpiry = true;
    private int flushMaxRows = DEFAULT_FLUSH_MAX_ROWS;
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
    private boolean compactionEnabled = true;
    private Duration compactionInterval = DEFAULT_COMPACTION_INTERVAL;

    /** Shards where a metric was logically deleted; compaction purges their dead rows, then clears them. */
    private final Set<Path> shardsPendingPurge = ConcurrentHashMap.newKeySet();

    /** Dropwizard metrics exported over JMX (see {@link ParquetMetrics}); registry lives for the
     *  bean's lifetime, the JMX reporter is started in {@link #init()} and stopped in {@link #destroy()}. */
    private final ParquetMetrics metrics = new ParquetMetrics();

    /** Built in {@link #init()} from {@link #baseDir} (or {@link #RRD_BASE_DIR_PROPERTY}). */
    private volatile PathMapper pathMapper;
    private volatile SampleWriter sampleWriter;
    private volatile SampleBuffer sampleBuffer;

    /**
     * Runs the retention sweep and compaction serially (single thread) so those two tree-mutating
     * jobs never race each other; the frequent buffer flush gets its own executor so it is never
     * blocked behind them. Started in {@link #init()}, stopped in {@link #destroy()}.
     */
    private ScheduledExecutorService maintenanceExecutor;
    private ScheduledExecutorService flushExecutor;
    /** Built in {@link #startMaintenance()} whether or not scheduled compaction is enabled, so the
     *  manual {@code tss-parquet:compact} command can always run a pass. */
    private volatile Compactor compactor;

    /** No-arg constructor for the OSGi blueprint; configuration arrives via setters before {@link #init()}. */
    public ParquetStorage() {
    }

    public ParquetStorage(final Path baseDir) {
        this(baseDir, DEFAULT_PARTITION_DURATION);
    }

    public ParquetStorage(final Path baseDir, final Duration partitionDuration) {
        this(baseDir, partitionDuration, DEFAULT_RETENTION, DEFAULT_RETENTION_SWEEP_INTERVAL);
    }

    public ParquetStorage(final Path baseDir, final Duration partitionDuration, final Duration retention,
                          final Duration retentionSweepInterval) {
        this.baseDir = baseDir;
        this.partitionDuration = Objects.requireNonNull(partitionDuration, "partitionDuration");
        this.retention = Objects.requireNonNull(retention, "retention");
        this.retentionSweepInterval = Objects.requireNonNull(retentionSweepInterval, "retentionSweepInterval");
    }

    // --- Configuration setters (blueprint / ConfigAdmin PID org.opennms.timeseries.parquet) ---
    // Empty/blank values are ignored so a placeholder default of "" leaves the built-in default.

    public void setBaseDir(final String baseDir) {
        if (isSet(baseDir)) {
            this.baseDir = Paths.get(baseDir.trim());
        }
    }

    public void setPartitionDuration(final String partitionDuration) {
        if (isSet(partitionDuration)) {
            this.partitionDuration = Duration.parse(partitionDuration.trim());
        }
    }

    public void setRetention(final String retention) {
        if (isSet(retention)) {
            this.retention = Duration.parse(retention.trim());
        }
    }

    public void setRetentionSweepInterval(final String retentionSweepInterval) {
        if (isSet(retentionSweepInterval)) {
            this.retentionSweepInterval = Duration.parse(retentionSweepInterval.trim());
        }
    }

    public void setCompression(final String compression) {
        if (isSet(compression)) {
            this.compression = CompressionCodecName.valueOf(compression.trim().toUpperCase(Locale.ROOT));
        }
    }

    public void setPruneCatalogOnExpiry(final boolean pruneCatalogOnExpiry) {
        this.pruneCatalogOnExpiry = pruneCatalogOnExpiry;
    }

    public void setFlushMaxRows(final String flushMaxRows) {
        if (isSet(flushMaxRows)) {
            final int value = Integer.parseInt(flushMaxRows.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("flush.maxRows must be positive: " + value);
            }
            this.flushMaxRows = value;
        }
    }

    public void setFlushInterval(final String flushInterval) {
        if (isSet(flushInterval)) {
            this.flushInterval = Duration.parse(flushInterval.trim());
        }
    }

    public void setCompactionEnabled(final boolean compactionEnabled) {
        this.compactionEnabled = compactionEnabled;
    }

    public void setCompactionInterval(final String compactionInterval) {
        if (isSet(compactionInterval)) {
            this.compactionInterval = Duration.parse(compactionInterval.trim());
        }
    }

    private static boolean isSet(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    /**
     * Called once by the OSGi container after construction (see {@code init-method} in the blueprint).
     * Resolves the base directory from {@link #RRD_BASE_DIR_PROPERTY} when one was not configured,
     * failing fast if neither is available, then starts the background retention sweep.
     */
    public void init() throws StorageException {
        Path dir = baseDir;
        if (dir == null) {
            final String base = System.getProperty(RRD_BASE_DIR_PROPERTY);
            if (!isSet(base)) {
                throw new StorageException("No baseDir configured and system property '"
                        + RRD_BASE_DIR_PROPERTY + "' is unset; cannot determine where to store data.");
            }
            dir = Paths.get(base.trim());
        }
        pathMapper = new PathMapper(dir);
        sampleWriter = new SampleWriter(compression);
        sampleBuffer = new SampleBuffer(flushMaxRows);
        metrics.start(); // publish JMX metrics under org.opennms.timeseries.impl.tss-parquet
        loadCatalog();
        startBufferFlush();
        startMaintenance();
        log.info("ParquetStorage initialized: data root {}, partition {}, retention {}/sweep {}, "
                        + "compression {}, pruneCatalogOnExpiry {}, flush {} rows/{}, compaction {} every {}, "
                        + "{} metric(s) loaded.",
                pathMapper.dataRoot(), partitionDuration, retention, retentionSweepInterval,
                compression, pruneCatalogOnExpiry, flushMaxRows, flushInterval,
                compactionEnabled ? "on" : "off", compactionInterval, catalog.size());
    }

    /** Rebuilds the in-memory catalog from the durable sidecar so metrics survive a restart. */
    private void loadCatalog() throws StorageException {
        try {
            catalog.putAll(catalogStore.read(pathMapper.catalogFile()));
            catalog.markClean(); // what we just loaded is already persisted
        } catch (final IOException e) {
            throw new StorageException("Failed to load metric catalog from " + pathMapper.catalogFile(), e);
        }
    }

    /** Persists the catalog to its sidecar if it has changed since the last flush. */
    private void flushCatalogIfDirty() throws StorageException {
        synchronized (catalogFlushLock) {
            if (!catalog.isDirty()) {
                return;
            }
            // Clear before snapshotting so a concurrent change re-dirties rather than being lost.
            catalog.markClean();
            try {
                catalogStore.write(pathMapper.catalogFile(), catalog.metrics());
            } catch (final IOException e) {
                catalog.markDirty(); // persistence failed; retry on the next change/flush
                throw new StorageException("Failed to persist metric catalog to " + pathMapper.catalogFile(), e);
            }
        }
    }

    /**
     * Stops the background retention sweep. Wired as the blueprint {@code destroy-method} so the
     * daemon thread is released when the bundle stops.
     */
    public void destroy() {
        shutdown(flushExecutor);
        flushExecutor = null;
        shutdown(maintenanceExecutor);
        maintenanceExecutor = null;
        compactor = null; // so a post-shutdown compactNow() fails fast rather than racing
        metrics.stop(); // unregister the JMX MBeans
        // Drain buffered samples and persist any pending catalog changes so a clean shutdown
        // loses nothing.
        if (pathMapper != null) {
            try {
                flush();
            } catch (final StorageException e) {
                log.warn("Failed to flush buffered samples on shutdown.", e);
            }
            try {
                flushCatalogIfDirty();
            } catch (final StorageException e) {
                log.warn("Failed to persist metric catalog on shutdown.", e);
            }
        }
    }

    private static void shutdown(final ScheduledExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * Writes every buffered sample to parquet immediately. Called on the flush timer, at shutdown,
     * and by the contract test's {@code waitForPersistingChanges()} to make writes observable.
     */
    public void flush() throws StorageException {
        final SampleBuffer buffer = sampleBuffer;
        if (buffer == null) {
            return;
        }
        writeBuckets(buffer.drainAll());
    }

    private void startMaintenance() {
        maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "parquet-tss-maintenance");
            thread.setDaemon(true);
            return thread;
        });

        final RetentionManager retentionManager = new RetentionManager(
                pathMapper, catalog, retention, partitionDuration, pruneCatalogOnExpiry, Instant::now);
        final long sweepSeconds = Math.max(1L, retentionSweepInterval.getSeconds());
        // First run after one interval; a sweep is cheap enough not to warrant running at startup.
        maintenanceExecutor.scheduleWithFixedDelay(() -> {
            try {
                metrics.incPartitionsDeleted(retentionManager.sweep());
            } catch (final Exception e) {
                log.warn("Retention sweep failed; will retry next interval.", e);
            }
        }, sweepSeconds, sweepSeconds, TimeUnit.SECONDS);

        // Build the compactor unconditionally so the manual tss-parquet:compact command works even
        // when scheduled compaction is disabled; only the timer is gated on compactionEnabled.
        compactor = new Compactor(pathMapper, catalog, sampleReader, sampleWriter, COMPACTION_MIN_FILES);
        if (compactionEnabled) {
            final long compactSeconds = Math.max(1L, compactionInterval.getSeconds());
            maintenanceExecutor.scheduleWithFixedDelay(this::runCompaction,
                    compactSeconds, compactSeconds, TimeUnit.SECONDS);
        }
    }

    /** The scheduled-timer entry point: run a pass, logging start/end, swallow-and-log failures. */
    private void runCompaction() {
        final long startNanos = System.nanoTime();
        log.info("Scheduled compaction starting.");
        try {
            final int modified = doCompaction();
            log.info("Scheduled compaction finished: {} partition(s) modified in {} ms.",
                    modified, (System.nanoTime() - startNanos) / 1_000_000L);
        } catch (final Exception e) {
            log.warn("Compaction failed; will retry next interval.", e);
        }
    }

    /**
     * One compaction pass. Snapshots and clears the pending-purge shards up front (a delete arriving
     * mid-run re-flags its shard for the next pass rather than being dropped); on failure the purge
     * set is restored so it is retried. Must run on {@link #maintenanceExecutor} so passes serialize.
     *
     * @return the number of partitions modified
     */
    private int doCompaction() throws IOException {
        final Compactor c = compactor;
        if (c == null) {
            return 0;
        }
        final Set<Path> toPurge = new HashSet<>(shardsPendingPurge);
        shardsPendingPurge.removeAll(toPurge);
        boolean ok = false;
        // Time every pass (scheduled and manual) into the JMX compaction timer.
        final com.codahale.metrics.Timer.Context timerContext = metrics.compactionTimer().time();
        try {
            final int modified = c.compact(toPurge);
            ok = true;
            metrics.incPartitionsCompacted(modified);
            return modified;
        } finally {
            timerContext.stop();
            if (!ok) {
                shardsPendingPurge.addAll(toPurge); // retry the purge next pass
            }
        }
    }

    /**
     * Runs a compaction pass on demand (the {@code tss-parquet:compact} shell command). The pass is
     * submitted to the single maintenance thread and awaited, so a manual run never races the
     * scheduled sweep/compaction.
     */
    @Override
    public int compactNow() throws StorageException {
        final ScheduledExecutorService exec = maintenanceExecutor;
        if (exec == null || compactor == null) {
            throw new StorageException("Compaction is unavailable: storage is not initialized.");
        }
        try {
            return exec.submit(this::doCompaction).get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("Manual compaction was interrupted.", e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            throw new StorageException("Manual compaction failed.", cause != null ? cause : e);
        } catch (final java.util.concurrent.RejectedExecutionException e) {
            throw new StorageException("Compaction is unavailable: storage is shutting down.", e);
        }
    }

    private void startBufferFlush() {
        flushExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "parquet-tss-flush");
            thread.setDaemon(true);
            return thread;
        });
        final long intervalSeconds = Math.max(1L, flushInterval.getSeconds());
        flushExecutor.scheduleWithFixedDelay(() -> {
            try {
                flush();
            } catch (final Exception e) {
                log.warn("Scheduled buffer flush failed; buffered samples remain and will retry.", e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /** Writes each drained bucket to a new parquet file. */
    private void writeBuckets(final Map<Path, List<SampleRow>> buckets) throws StorageException {
        for (final Map.Entry<Path, List<SampleRow>> bucket : buckets.entrySet()) {
            writeBucket(bucket.getKey(), bucket.getValue());
        }
    }

    private void writeBucket(final Path partitionDir, final List<SampleRow> rows) throws StorageException {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        // A write to a not-yet-existing partition dir creates it; count that as a new partition.
        // (Approximate under concurrent first-writes to the same partition; fine for an ops gauge.)
        final boolean newPartition = !Files.isDirectory(partitionDir);
        try {
            sampleWriter.write(partitionDir, rows);
        } catch (final IOException e) {
            throw new StorageException("Failed to write samples to " + partitionDir, e);
        }
        if (newPartition) {
            metrics.incPartitionsCreated();
        }
    }

    @Override
    public void store(final List<Sample> samples) throws StorageException {
        Objects.requireNonNull(samples, "samples");
        if (samples.isEmpty()) {
            return;
        }
        final PathMapper paths = requireInitialized();
        final SampleBuffer buffer = sampleBuffer;
        metrics.incSamplesWritten(samples.size());

        // Buffer each sample under its target partition (shard + time partition), indexing each
        // distinct metric in the catalog as we go. A parquet file is written only when a bucket
        // reaches the row threshold (or on the flush timer / at shutdown), coalescing many small
        // writes into fewer, larger files.
        final Set<Path> bucketsToFlush = new HashSet<>();
        for (final Sample sample : samples) {
            final Metric metric = sample.getMetric();
            catalog.put(metric);
            final Path partitionDir = paths.partitionDir(resourceId(metric), sample.getTime(), partitionDuration);
            final SampleRow row = new SampleRow(sample.getTime(), name(metric), sample.getValue());
            if (buffer.add(partitionDir, row)) {
                bucketsToFlush.add(partitionDir);
            }
        }

        // Eagerly flush only the buckets that crossed the threshold.
        for (final Path partitionDir : bucketsToFlush) {
            writeBucket(partitionDir, buffer.drainBucket(partitionDir));
        }

        // Persist the catalog only if this batch introduced a new metric (or changed one's
        // meta/external tags); the common all-known-metrics case leaves it clean and skips I/O.
        flushCatalogIfDirty();
    }

    /**
     * Bulk-imports samples (the {@code tss-parquet:import-from-rrd} command), bypassing the async
     * buffer. Rows are grouped by {@code (shard,partition)} and each partition's rows are written as
     * ONE parquet file. For a greenfield historical backfill that yields the optimal
     * one-file-per-partition layout with no compaction; a partition that already holds live data
     * simply gains one more file, which a later compaction pass consolidates. Each distinct metric is
     * indexed in the catalog, but the durable sidecar is left for a single {@link #flushCatalog()} at
     * the end of the import so a per-resource import loop doesn't rewrite the whole sidecar each call.
     *
     * @return the number of partition files written
     */
    @Override
    public int bulkImport(final Collection<Sample> samples) throws StorageException {
        Objects.requireNonNull(samples, "samples");
        if (samples.isEmpty()) {
            return 0;
        }
        final PathMapper paths = requireInitialized();
        metrics.incSamplesWritten(samples.size());

        final Map<Path, List<SampleRow>> byPartition = new HashMap<>();
        for (final Sample sample : samples) {
            final Metric metric = sample.getMetric();
            catalog.put(metric);
            final Path partitionDir = paths.partitionDir(resourceId(metric), sample.getTime(), partitionDuration);
            byPartition.computeIfAbsent(partitionDir, k -> new ArrayList<>())
                    .add(new SampleRow(sample.getTime(), name(metric), sample.getValue()));
        }

        int partitionsWritten = 0;
        for (final Map.Entry<Path, List<SampleRow>> entry : byPartition.entrySet()) {
            writeBucket(entry.getKey(), entry.getValue());
            partitionsWritten++;
        }
        return partitionsWritten;
    }

    /** Persists any pending catalog (metric index) changes to the durable sidecar; call once after a bulk import. */
    @Override
    public void flushCatalog() throws StorageException {
        requireInitialized();
        flushCatalogIfDirty();
    }

    /** The plugin's metric registry (also exported over JMX); used by the {@code tss-parquet:stats} command. */
    @Override
    public com.codahale.metrics.MetricRegistry metricRegistry() {
        return metrics.registry();
    }

    @Override
    public List<Metric> findMetrics(final Collection<TagMatcher> matchers) throws StorageException {
        // Matchers support EQUALS, NOT_EQUALS, EQUALS_REGEX and NOT_EQUALS_REGEX, evaluated
        // over the intrinsic and meta tags of every catalogued metric (external tags excluded).
        return catalog.findMetrics(matchers);
    }

    @Override
    public TimeSeriesData getTimeSeriesData(final TimeSeriesFetchRequest request) throws StorageException {
        Objects.requireNonNull(request, "request");
        if (request.getAggregation() != Aggregation.NONE) {
            throw new IllegalArgumentException(
                    String.format("Aggregation %s is not supported; OpenNMS aggregates in memory.",
                            request.getAggregation()));
        }
        final PathMapper paths = requireInitialized();

        // Resolve the full, catalogued metric (the request's metric may carry intrinsic tags only).
        // A catalog miss means the metric is unknown or was logically deleted => no data.
        final Metric metric = catalog.get(request.getMetric().getKey());
        if (metric == null) {
            return ImmutableTimeSeriesData.builder()
                    .metric(request.getMetric())
                    .dataPoints(List.of())
                    .build();
        }

        final Path shardDir = paths.shardDir(resourceId(metric));
        final List<SampleRow> rows;
        try {
            rows = sampleReader.read(shardDir, name(metric), request.getStart(), request.getEnd());
        } catch (final IOException e) {
            throw new StorageException("Failed to read samples for metric " + metric.getKey(), e);
        }
        metrics.incSamplesRead(rows.size());

        final List<DataPoint> dataPoints = rows.stream()
                .sorted(Comparator.comparing(SampleRow::time))
                .map(r -> (DataPoint) new ImmutableDataPoint(r.time(), r.value()))
                .collect(Collectors.toList());
        return ImmutableTimeSeriesData.builder().metric(metric).dataPoints(dataPoints).build();
    }

    @Override
    public List<Sample> getTimeseries(final TimeSeriesFetchRequest request) throws StorageException {
        // Deprecated raw-sample view, kept consistent with getTimeSeriesData.
        final TimeSeriesData data = getTimeSeriesData(request);
        final Metric metric = data.getMetric();
        return data.getDataPoints().stream()
                .map(dp -> (Sample) ImmutableSample.builder()
                        .metric(metric)
                        .time(dp.getTime())
                        .value(dp.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public void delete(final Metric metric) throws StorageException {
        Objects.requireNonNull(metric, "metric");
        // Logical delete: drop it from the catalog so it vanishes from findMetrics and
        // getTimeSeriesData. Flag its shard so the next compaction pass physically purges the
        // metric's rows (DESIGN §7). No error if the metric is unknown.
        catalog.remove(metric);
        final PathMapper paths = pathMapper;
        final Tag resourceId = metric.getFirstTagByKey(IntrinsicTagNames.resourceId);
        if (paths != null && resourceId != null) {
            shardsPendingPurge.add(paths.shardDir(resourceId.getValue()));
        }
        flushCatalogIfDirty(); // persist the removal so it survives a restart
    }

    @Override
    public boolean supportsAggregation(final Aggregation aggregation) {
        // Pure-JVM parquet cannot push down aggregation; OpenNMS aggregates raw samples in memory.
        return aggregation == Aggregation.NONE;
    }

    private PathMapper requireInitialized() throws StorageException {
        final PathMapper paths = pathMapper;
        if (paths == null) {
            throw new StorageException("ParquetStorage is not initialized: call init() or supply a baseDir.");
        }
        return paths;
    }

    private static String resourceId(final Metric metric) throws StorageException {
        final Tag tag = metric.getFirstTagByKey(IntrinsicTagNames.resourceId);
        if (tag == null) {
            throw new StorageException("Metric has no '" + IntrinsicTagNames.resourceId
                    + "' intrinsic tag: " + metric.getKey());
        }
        return tag.getValue();
    }

    private static String name(final Metric metric) {
        final Tag tag = metric.getFirstTagByKey(IntrinsicTagNames.name);
        return tag == null ? "" : tag.getValue();
    }
}