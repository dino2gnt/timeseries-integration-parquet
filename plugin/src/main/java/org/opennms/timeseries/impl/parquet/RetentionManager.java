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
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces the data-retention lifecycle that Parquet cannot manage itself (DESIGN §8).
 *
 * <p>A {@link #sweep()} walks the sharded data tree, deletes whole {@code p=<partition>}
 * directories whose time window has fully aged past the retention cutoff, removes any shard
 * directories that are left empty, and finally prunes catalog entries for series that retain no
 * partitions. Whole-directory deletion is cheap and safe because each partition's parquet files
 * are self-contained.</p>
 *
 * <p>A partition {@code [start, start+partitionDuration)} is expired when its <em>end</em> is at
 * or before {@code now - retention}; i.e. every sample it could hold is older than the retention
 * window. This never deletes data that is still within retention.</p>
 *
 * <p>The current wall-clock is supplied via a {@link Supplier} so sweeps are deterministic under
 * test.</p>
 */
class RetentionManager {

    private static final Logger log = LoggerFactory.getLogger(RetentionManager.class);

    private final PathMapper pathMapper;
    private final MetricCatalog catalog;
    private final Duration retention;
    private final Duration partitionDuration;
    private final boolean pruneCatalog;
    private final Supplier<Instant> clock;

    RetentionManager(final PathMapper pathMapper, final MetricCatalog catalog, final Duration retention,
                     final Duration partitionDuration, final boolean pruneCatalog, final Supplier<Instant> clock) {
        this.pathMapper = Objects.requireNonNull(pathMapper, "pathMapper");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.retention = Objects.requireNonNull(retention, "retention");
        this.partitionDuration = Objects.requireNonNull(partitionDuration, "partitionDuration");
        this.pruneCatalog = pruneCatalog;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Runs one retention pass.
     *
     * @return the number of partition directories that were deleted
     */
    int sweep() throws IOException {
        final Path dataRoot = pathMapper.dataRoot();
        if (!Files.isDirectory(dataRoot)) {
            return 0;
        }
        final Instant cutoff = clock.get().minus(retention);

        final List<Path> expired = findExpiredPartitions(dataRoot, cutoff);
        for (final Path partitionDir : expired) {
            deleteRecursively(partitionDir);
        }
        deleteEmptyDirectoriesBelow(dataRoot);
        final int pruned = pruneCatalog ? pruneCatalog() : 0;

        if (!expired.isEmpty() || pruned > 0) {
            log.info("Retention sweep (cutoff {}) removed {} expired partition(s) and pruned {} metric(s).",
                    cutoff, expired.size(), pruned);
        }
        return expired.size();
    }

    private List<Path> findExpiredPartitions(final Path dataRoot, final Instant cutoff) throws IOException {
        final List<Path> expired = new ArrayList<>();
        Files.walkFileTree(dataRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                if (dir.equals(dataRoot)) {
                    return FileVisitResult.CONTINUE;
                }
                final Optional<Instant> start = pathMapper.parsePartitionStart(dir.getFileName().toString());
                if (start.isEmpty()) {
                    return FileVisitResult.CONTINUE; // an intermediate shard directory; descend into it
                }
                // A partition directory: decide its fate and never descend (partitions don't nest).
                final Instant end = start.get().plus(partitionDuration);
                if (!end.isAfter(cutoff)) {
                    expired.add(dir);
                }
                return FileVisitResult.SKIP_SUBTREE;
            }
        });
        return expired;
    }

    private int pruneCatalog() {
        int pruned = 0;
        for (final Metric metric : catalog.metrics()) {
            final Tag resourceId = metric.getFirstTagByKey(IntrinsicTagNames.resourceId);
            if (resourceId == null) {
                continue;
            }
            if (!hasAnyPartition(pathMapper.shardDir(resourceId.getValue()))) {
                catalog.remove(metric);
                pruned++;
            }
        }
        return pruned;
    }

    private boolean hasAnyPartition(final Path shardDir) {
        if (!Files.isDirectory(shardDir)) {
            return false;
        }
        try (DirectoryStream<Path> partitions =
                     Files.newDirectoryStream(shardDir, PathMapper.PARTITION_PREFIX + "*")) {
            for (final Path partition : partitions) {
                if (Files.isDirectory(partition)) {
                    return true;
                }
            }
            return false;
        } catch (final IOException e) {
            // Be conservative: on an I/O error, keep the catalog entry rather than drop it.
            log.warn("Could not list partitions under {}; leaving its catalog entries in place.", shardDir, e);
            return true;
        }
    }

    /** Recursively deletes a directory tree (files first, then directories). */
    static void deleteRecursively(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(RetentionManager::deleteUnchecked);
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Removes every empty directory strictly below {@code dataRoot}, cascading bottom-up. */
    private void deleteEmptyDirectoriesBelow(final Path dataRoot) throws IOException {
        try (Stream<Path> walk = Files.walk(dataRoot)) {
            walk.sorted(Comparator.reverseOrder()) // deepest first, so parents empty out after children
                    .filter(p -> !p.equals(dataRoot))
                    .filter(Files::isDirectory)
                    .forEach(dir -> {
                        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
                            if (!entries.iterator().hasNext()) {
                                Files.delete(dir);
                            }
                        } catch (final IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static void deleteUnchecked(final Path path) {
        try {
            Files.delete(path);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}