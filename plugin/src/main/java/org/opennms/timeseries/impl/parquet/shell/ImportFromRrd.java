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

package org.opennms.timeseries.impl.parquet.shell;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.timeseries.impl.parquet.ParquetMaintenance;
import org.opennms.timeseries.impl.parquet.util.rrd.AbstractDS;
import org.opennms.timeseries.impl.parquet.util.rrd.AbstractRRA;
import org.opennms.timeseries.impl.parquet.util.rrd.AbstractRRD;
import org.opennms.timeseries.impl.parquet.util.rrd.ResourcePath;
import org.opennms.timeseries.impl.parquet.util.rrd.Row;
import org.opennms.timeseries.impl.parquet.util.rrd.RrdConvertUtils;

/**
 * {@code tss-parquet:import-from-rrd} &mdash; backfills the parquet store from local RRDtool
 * ({@code .rrd}) or JRobin ({@code .jrb}) files under {@code rrd.base.dir}.
 *
 * <p>Ported from the pgtimeseries {@code backfill-from-rrd} command, but samples are written through
 * {@link ParquetMaintenance#bulkImport} rather than the async buffer: an import knows a partition's
 * whole set of samples up front, so each {@code (shard,partition)} is written as a single parquet
 * file (a greenfield backfill needs no compaction; a partition that already holds live data gains one
 * file the scheduled compaction later merges). See {@code DESIGN.md} for the store()-vs-bulk analysis.</p>
 *
 * <p>Like the shell package's other commands, this lives in {@code ...parquet.shell} so the bundle's
 * narrow {@code Karaf-Commands} header scans only command classes, never the embedded jars.</p>
 */
@Command(scope = "tss-parquet", name = "import-from-rrd",
        description = "Backfill time series metrics from local RRD (.rrd) or JRobin (.jrb) files.")
@Service
public class ImportFromRrd implements Action {

    @Option(name = "-t", aliases = "--threads",
            description = "Number of import threads. Defaults to the number of available processors.")
    int threads = Runtime.getRuntime().availableProcessors();

    @Option(name = "-v", aliases = "--verbose", description = "Be verbose; show per-resource output.")
    boolean verbose = false;

    @Option(name = "-s", aliases = "--storage-tool", required = true,
            description = "Storage tool to migrate from: '[rrdtool|rrd|jrobin|jrb]'.")
    String tool = "rrd";

    @Option(name = "--yes-really",
            description = "Confirm you really want to read and import all local rrd/jrb data.")
    boolean yesreally = false;

    @Reference
    private DataSource dataSource;

    @Reference
    private ParquetMaintenance storage;

    private final Path rrdDir = Paths.get(System.getProperty("rrd.base.dir", ""));
    private final boolean storeByGroup = Boolean.parseBoolean(System.getProperty("org.opennms.rrd.storeByGroup"));
    // Honor the live resourceId scheme: only when storeByForeignSource=true is a store-by-id RRD path
    // (snmp/<nodeId>/...) rewritten to snmp/fs/<fs>/<fid>/...; otherwise the on-disk path is used
    // verbatim so imported resourceIds match what live collection writes.
    private final boolean storeByForeignSource =
            Boolean.parseBoolean(System.getProperty("org.opennms.rrd.storeByForeignSource"));

    private enum StorageStrategy { STORE_BY_METRIC, STORE_BY_GROUP }
    private enum StorageTool { RRDTOOL, JROBIN }

    private StorageStrategy storageStrategy;
    private StorageTool storageTool;
    private ExecutorService executor;
    private final Map<Integer, ForeignId> foreignIds = new HashMap<>();

    private final LongAdder resourcesProcessed = new LongAdder();
    private final LongAdder samplesImported = new LongAdder();
    private final LongAdder filesWritten = new LongAdder();

    @Override
    public Object execute() {
        if (!yesreally) {
            System.out.println();
            System.out.println("This utility performs significant work and can severely bloat and/or pollute your "
                    + "timeseries store. Providing '--yes-really' shows you understand the risks and is required.");
            System.out.println();
            return null;
        }
        if (rrdDir.toString().isEmpty() || !Files.isDirectory(rrdDir)) {
            System.out.println("rrd.base.dir is not set or does not exist: '" + rrdDir + "'.");
            return null;
        }

        storageStrategy = storeByGroup ? StorageStrategy.STORE_BY_GROUP : StorageStrategy.STORE_BY_METRIC;
        System.out.println("Using " + (storeByGroup ? "storeByGroup" : "store by metric") + " strategy.");

        if (Objects.equals(tool, "rrd") || Objects.equals(tool, "rrdtool")) {
            storageTool = StorageTool.RRDTOOL;
            System.out.println("Using RRDtool strategy.");
        } else if (Objects.equals(tool, "jrb") || Objects.equals(tool, "jrobin")) {
            storageTool = StorageTool.JROBIN;
            System.out.println("Using JRobin strategy.");
        } else {
            System.out.println();
            System.out.println("Invalid storage tool selected. Must be one of 'rrdtool', 'rrd', 'jrobin', or 'jrb'.");
            System.out.println();
            return null;
        }

        System.out.println("Using storeByForeignSource=" + storeByForeignSource + ".");

        // The node table is only needed to translate store-by-id RRD paths into store-by-foreign-source
        // resourceIds; skip it entirely when storeByForeignSource=false (paths are used verbatim).
        if (storeByForeignSource) {
            try (final Connection conn = dataSource.getConnection();
                 final Statement st = conn.createStatement();
                 final ResultSet rs = st.executeQuery("SELECT nodeid, foreignsource, foreignid FROM node")) {
                while (rs.next()) {
                    foreignIds.put(rs.getInt("nodeid"),
                            new ForeignId(rs.getString("foreignsource"), rs.getString("foreignid")));
                }
            } catch (final Exception e) {
                System.out.println("Failed to read node table: " + e);
                return null;
            }
            System.out.println("Found " + foreignIds.size() + " nodes in the database.");
        }

        System.out.println("Using " + threads + " threads for import.");
        this.executor = new ForkJoinPool(threads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

        try {
            System.out.print("Starting import.");
            this.processStoreByGroupResources(this.rrdDir.resolve("response"));
            switch (storageStrategy) {
                case STORE_BY_GROUP:
                    this.processStoreByGroupResources(this.rrdDir.resolve("snmp"));
                    break;
                case STORE_BY_METRIC:
                    this.processStoreByMetricResources(this.rrdDir.resolve("snmp"));
                    break;
            }
        } catch (final Exception e) {
            System.out.println("Import error: " + e);
        } finally {
            this.executor.shutdown();
            boolean done = false;
            while (!done) {
                try {
                    done = this.executor.awaitTermination(3, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                System.out.print(".");
            }
            // All import threads have written their files directly; now persist the catalog once so
            // every imported metric survives a restart and is findable.
            try {
                this.storage.flushCatalog();
            } catch (final StorageException e) {
                System.out.println("Catalog flush failed; imported metrics may not be persisted: " + e);
            }
            System.out.println();
            System.out.println("Import complete: " + resourcesProcessed.sum() + " resource(s), "
                    + samplesImported.sum() + " sample(s), " + filesWritten.sum() + " partition file(s) written.");
            System.out.println();
        }
        return null;
    }

    private void processStoreByGroupResources(final Path path) {
        if (!Files.isDirectory(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.filter(p -> p.endsWith("ds.properties"))
                    .forEach(p -> processStoreByGroupResource(p.getParent()));
        } catch (final Exception e) {
            System.out.println("Error while reading RRD files under " + path + ": " + e);
        }
    }

    private void processStoreByGroupResource(final Path path) {
        final Properties ds = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(path.resolve("ds.properties"))) {
            ds.load(r);
        } catch (final IOException e) {
            return;
        }
        final Set<String> groups = ds.values().stream().map(Object::toString).collect(Collectors.toSet());
        groups.forEach(group -> this.executor.execute(() -> processResource(path, group, group)));
    }

    private void processStoreByMetricResources(final Path path) {
        if (!Files.isDirectory(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.filter(p -> p.getFileName().toString().endsWith(".meta"))
                    .forEach(this::processStoreByMetricResource);
        } catch (final Exception e) {
            System.out.println("Error while reading RRD files under " + path + ": " + e);
        }
    }

    private void processStoreByMetricResource(final Path metaPath) {
        final Path path = metaPath.getParent();
        final String metric = removeExtension(metaPath.getFileName().toString());

        final Properties meta = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(metaPath)) {
            meta.load(r);
        } catch (final IOException e) {
            System.out.println("Failed to read .meta file '" + metaPath + "': " + e);
            return;
        }
        final String group = meta.getProperty("GROUP");
        if (group == null) {
            return;
        }
        this.executor.execute(() -> this.processResource(path, metric, group));
    }

    private void processResource(final Path resourceDir, final String fileName, final String group) {
        if (verbose) {
            System.out.println("Processing resource: dir='" + resourceDir + "', file='" + fileName + "', group='" + group + "'");
        }
        final ResourcePath resourcePath = buildResourcePath(resourceDir);
        if (resourcePath == null) {
            return;
        }

        final Properties properties = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(resourceDir.resolve("strings.properties"))) {
            properties.load(r);
        } catch (final IOException e) {
            // strings.properties is optional
        }

        final Path file = resourceDir.resolve(fileName + (storageTool == StorageTool.RRDTOOL ? ".rrd" : ".jrb"));
        if (!Files.exists(file)) {
            System.out.println("File not found: " + file);
            return;
        }

        final AbstractRRD rrd;
        try {
            rrd = storageTool == StorageTool.RRDTOOL
                    ? RrdConvertUtils.dumpRrd(file.toFile())
                    : RrdConvertUtils.dumpJrb(file.toFile());
        } catch (final Exception e) {
            System.out.println("Can't parse JRB/RRD file '" + file + "': " + e);
            return;
        }

        try {
            injectSamples(resourcePath, group, rrd.getDataSources(), generateSamples(rrd), properties);
            resourcesProcessed.increment();
        } catch (final Exception e) {
            System.out.println("Failed to convert file '" + file + "': " + e);
        }
    }

    private Sample toSample(final AbstractDS ds, final ResourcePath resourcePath, final String group,
                           final Instant timestamp, final double value, final Properties props) {
        final ImmutableMetric.MetricBuilder metric = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.resourceId, resourcePath.toString() + "/" + group)
                .intrinsicTag(IntrinsicTagNames.name, ds.getName())
                .metaTag("mtype", ds.isCounter() ? "count" : "gauge");
        if (props != null && !props.isEmpty()) {
            for (final String k : props.stringPropertyNames()) {
                metric.externalTag(k, props.getProperty(k));
            }
        }
        return ImmutableSample.builder().time(timestamp).value(value).metric(metric.build()).build();
    }

    private void injectSamples(final ResourcePath resourcePath, final String group,
                              final List<? extends AbstractDS> dataSources,
                              final SortedMap<Long, List<Double>> samples, final Properties props)
            throws StorageException {
        final List<Sample> batch = new ArrayList<>();
        for (final Map.Entry<Long, List<Double>> s : samples.entrySet()) {
            for (int i = 0; i < dataSources.size(); i++) {
                final double value = s.getValue().get(i);
                if (Double.isNaN(value)) {
                    continue;
                }
                final Instant timestamp = Instant.ofEpochSecond(s.getKey());
                try {
                    batch.add(toSample(dataSources.get(i), resourcePath, group, timestamp, value, props));
                } catch (final IllegalArgumentException e) {
                    // value outside the expected range for its type (e.g. negative counter); skip
                }
            }
        }
        if (!batch.isEmpty()) {
            // One bulk write per resource: samples are grouped by (shard,partition) and each partition
            // written as a single file. Catalog persistence is deferred to the final flushCatalog().
            filesWritten.add(this.storage.bulkImport(batch));
            samplesImported.add(batch.size());
        }
    }

    private ResourcePath buildResourcePath(final Path resourceDir) {
        return buildResourcePath(this.rrdDir.relativize(resourceDir), this.storeByForeignSource, this.foreignIds);
    }

    /**
     * Maps an on-disk RRD resource directory (relative to {@code rrd.base.dir}) to its OpenNMS
     * resourceId path. Only when {@code storeByForeignSource} is true is a store-by-id path
     * ({@code snmp/<nodeId>/...}) rewritten to store-by-foreign-source
     * ({@code snmp/fs/<foreignSource>/<foreignId>/...}) via the node map; otherwise the path is used
     * verbatim so imported resourceIds match what live collection writes. Paths already under
     * {@code snmp/fs} and non-snmp paths (e.g. {@code response/}) are always kept as-is. Returns
     * {@code null} if a store-by-id node has no foreign-source mapping.
     */
    static ResourcePath buildResourcePath(final Path relativeResourceDir,
                                          final boolean storeByForeignSource,
                                          final Map<Integer, ForeignId> foreignIds) {
        if (storeByForeignSource
                && relativeResourceDir.startsWith(Paths.get("snmp"))
                && !relativeResourceDir.startsWith(Paths.get("snmp", "fs"))) {
            final int nodeId = Integer.parseInt(relativeResourceDir.getName(1).toString());
            final ForeignId foreignId = foreignIds.get(nodeId);
            if (foreignId == null) {
                return null;
            }
            final List<String> tail = new ArrayList<>();
            for (int i = 2; i < relativeResourceDir.getNameCount(); i++) {
                tail.add(relativeResourceDir.getName(i).toString());
            }
            return ResourcePath.get(
                    ResourcePath.get(ResourcePath.get("snmp", "fs"), foreignId.foreignSource, foreignId.foreignId),
                    tail);
        }

        final List<String> all = new ArrayList<>();
        for (final Path p : relativeResourceDir) {
            all.add(p.toString());
        }
        return ResourcePath.get(all);
    }

    private static String removeExtension(final String filename) {
        final int dot = filename.lastIndexOf('.');
        return dot < 0 ? filename : filename.substring(0, dot);
    }

    // --- RRD sample reconstruction (ported verbatim from the pgtimeseries backfill command) ---------

    public static SortedMap<Long, List<Double>> generateSamples(final AbstractRRD rrd) {
        // With pdpPerRow = 1 the consolidation function is a no-op (cf([x]) == x), so that RRA gives
        // the finest data over the longest range regardless of its CF; fill the rest from AVERAGE RRAs.
        final NavigableSet<AbstractRRA> rras = Stream.concat(
                        rrd.getRras().stream()
                                .filter(rra -> rra.getPdpPerRow() == 1L)
                                .max(Comparator.comparingInt(rra -> rra.getRows().size()))
                                .map(Stream::of).orElseGet(Stream::empty),
                        rrd.getRras().stream().filter(AbstractRRA::hasAverageAsCF))
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(AbstractRRA::getPdpPerRow))));

        final SortedMap<Long, List<Double>> collected = new TreeMap<>();
        for (final AbstractRRA rra : rras) {
            NavigableMap<Long, List<Double>> samples = generateSamples(rrd, rra);

            final AbstractRRA lowerRra = rras.lower(rra);
            if (lowerRra != null) {
                final long lowerRraStart = rrd.getLastUpdate() - lowerRra.getPdpPerRow() * rrd.getStep() * lowerRra.getRows().size();
                final long rraStep = rra.getPdpPerRow() * rrd.getStep();
                samples = samples.headMap((int) Math.ceil(((double) lowerRraStart) / ((double) rraStep)) * rraStep, false);
            }
            if (samples.isEmpty()) {
                continue;
            }
            final AbstractRRA higherRra = rras.higher(rra);
            if (higherRra != null) {
                final long higherRraStep = higherRra.getPdpPerRow() * rrd.getStep();
                samples = samples.tailMap((int) Math.ceil(((double) samples.firstKey()) / ((double) higherRraStep)) * higherRraStep, true);
            }
            collected.putAll(samples);
        }
        return collected;
    }

    public static NavigableMap<Long, List<Double>> generateSamples(final AbstractRRD rrd, final AbstractRRA rra) {
        final long step = rra.getPdpPerRow() * rrd.getStep();
        final long start = rrd.getStartTimestamp(rra);
        final long end = rrd.getEndTimestamp(rra);

        final NavigableMap<Long, List<Double>> valuesMap = new TreeMap<>();
        for (long ts = start; ts <= end; ts += step) {
            final List<Double> values = new ArrayList<>();
            for (int i = 0; i < rrd.getDataSources().size(); i++) {
                values.add(Double.NaN);
            }
            valuesMap.put(ts, values);
        }

        final List<Double> lastValues = new ArrayList<>();
        for (final AbstractDS ds : rrd.getDataSources()) {
            final double v = ds.getLastDs() == null ? 0.0 : ds.getLastDs();
            lastValues.add(v - v % step);
        }
        for (int i = 0; i < rrd.getDataSources().size(); i++) {
            if (rrd.getDataSource(i).isCounter()) {
                valuesMap.get(end).set(i, lastValues.get(i));
            }
        }

        // Counters are reconstructed newest-to-oldest to recover raw values from rate deltas.
        long ts = end - step;
        for (int j = rra.getRows().size() - 1; j >= 0; j--) {
            final Row row = rra.getRows().get(j);
            for (int i = 0; i < rrd.getDataSources().size(); i++) {
                if (rrd.getDataSource(i).isCounter()) {
                    if (j > 0) {
                        final Double last = lastValues.get(i);
                        final Double current = row.getValue(i).isNaN() ? 0 : row.getValue(i);
                        Double value = last - (current * step);
                        if (value < 0) { // counter-wrap emulation
                            value += Math.pow(2, 64);
                        }
                        lastValues.set(i, value);
                        if (!row.getValue(i).isNaN()) {
                            valuesMap.get(ts).set(i, value);
                        }
                    }
                } else {
                    if (!row.getValue(i).isNaN()) {
                        valuesMap.get(ts + step).set(i, row.getValue(i));
                    }
                }
            }
            ts -= step;
        }
        return valuesMap;
    }

    static final class ForeignId {
        private final String foreignSource;
        private final String foreignId;

        ForeignId(final String foreignSource, final String foreignId) {
            this.foreignSource = foreignSource;
            this.foreignId = foreignId;
        }
    }
}