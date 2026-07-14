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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges the many small {@code part-*.parquet} files a partition accumulates (one per flush) into
 * a single larger file, and &mdash; for shards where a metric was logically deleted &mdash; drops
 * the deleted metrics' rows while rewriting.
 *
 * <p>A frequent flush keeps memory and the crash window small but produces one file per collection
 * cycle per shard; left alone that is hundreds of tiny files per shard per day, which Parquet reads
 * poorly. This periodic merge is the counterpart that keeps file sizes healthy (the standard
 * columnar flush-small / compact-later split).</p>
 *
 * <p><b>Safety.</b> A partition is rewritten when it has {@code >= minFiles} files (a merge) or its
 * shard is flagged for purge. Only the files enumerated at the start of that partition's pass are
 * read and then deleted, so a sample the flusher writes concurrently (a new file) is never lost.
 * The merged output is written to a temp file that does not match the {@code part-*} read glob and
 * then moved into place, so readers never observe duplicated rows. Merge mode keeps every row;
 * purge mode only drops rows whose metric is no longer live in the catalog, so an incomplete
 * catalog can never cause a plain merge to lose data.</p>
 */
class Compactor {

    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private static final String COMPACT_TEMP_PREFIX = ".compact-";
    private static final String COMPACT_TEMP_SUFFIX = ".tmp";

    private final PathMapper pathMapper;
    private final MetricCatalog catalog;
    private final SampleReader sampleReader;
    private final SampleWriter sampleWriter;
    private final int minFiles;

    Compactor(final PathMapper pathMapper, final MetricCatalog catalog, final SampleReader sampleReader,
              final SampleWriter sampleWriter, final int minFiles) {
        this.pathMapper = Objects.requireNonNull(pathMapper, "pathMapper");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.sampleReader = Objects.requireNonNull(sampleReader, "sampleReader");
        this.sampleWriter = Objects.requireNonNull(sampleWriter, "sampleWriter");
        if (minFiles < 2) {
            throw new IllegalArgumentException("minFiles must be at least 2: " + minFiles);
        }
        this.minFiles = minFiles;
    }

    /**
     * Runs one compaction pass.
     *
     * @param shardsToPurge shards where a metric was deleted; their dead rows are dropped as their
     *                      partitions are rewritten
     * @return the number of partitions that were rewritten
     */
    int compact(final Set<Path> shardsToPurge) throws IOException {
        final Path dataRoot = pathMapper.dataRoot();
        if (!Files.isDirectory(dataRoot)) {
            return 0;
        }
        final Map<Path, Set<String>> liveNamesByShard = liveNamesForShards(shardsToPurge);

        int compacted = 0;
        for (final Path partitionDir : findPartitionDirs(dataRoot)) {
            final Path shardDir = partitionDir.getParent();
            final boolean purge = shardsToPurge.contains(shardDir);
            try {
                if (compactPartition(partitionDir, purge, liveNamesByShard.getOrDefault(shardDir, Set.of()))) {
                    compacted++;
                }
            } catch (final RuntimeException | IOException e) {
                // A single bad partition shouldn't abort the whole pass; it retries next run. Catch
                // RuntimeException too: parquet's reader throws unchecked (e.g. "not a Parquet file")
                // on a corrupt file, which must not escape and kill compaction of every other shard.
                log.warn("Failed to compact partition {}; skipping.", partitionDir, e);
            }
        }
        return compacted;
    }

    /** @return true if the partition was modified (empty files removed and/or files merged) */
    private boolean compactPartition(final Path partitionDir, final boolean purge,
                                     final Set<String> liveNames) throws IOException {
        // Drop 0-byte part files outright: with atomic writes a 0-byte part-*.parquet can only be a
        // crashed/interrupted or pre-atomic-write leftover and holds no rows. Removing them here both
        // heals the on-disk mess and stops a bad file from failing the read below.
        int removedEmpty = 0;
        final List<Path> realFiles = new ArrayList<>();
        for (final Path file : listPartFiles(partitionDir)) {
            if (Files.size(file) == 0) {
                Files.deleteIfExists(file);
                removedEmpty++;
            } else {
                realFiles.add(file);
            }
        }
        if (removedEmpty > 0) {
            log.warn("Removed {} empty parquet part file(s) in {}.", removedEmpty, partitionDir);
        }

        final List<SampleRow> merged = new ArrayList<>();
        final List<Path> readFiles = new ArrayList<>();
        for (final Path file : realFiles) {
            final List<SampleRow> rows;
            try {
                rows = sampleReader.readAllRows(file);
            } catch (final RuntimeException | IOException e) {
                // A corrupt, non-empty file can't be proven empty, so we neither merge nor delete it;
                // skip it (leaving it in place, flagged again next pass) so the readable files still
                // compact instead of the whole partition erroring out.
                log.warn("Skipping unreadable parquet file {} during compaction.", file, e);
                continue;
            }
            for (final SampleRow row : rows) {
                if (!purge || liveNames.contains(row.name())) {
                    merged.add(row);
                }
            }
            readFiles.add(file);
        }

        // Only rewrite when it's worth it: a real merge (>= minFiles readable files) or a purge that
        // actually has something to rewrite. Otherwise we'd churn a lone good file next to a skipped
        // corrupt one on every pass.
        final boolean worthRewriting = purge ? !readFiles.isEmpty() : readFiles.size() >= minFiles;
        if (!worthRewriting) {
            return removedEmpty > 0;
        }

        if (merged.isEmpty()) {
            // Everything we could read was purged: drop exactly the files we read.
            for (final Path file : readFiles) {
                Files.deleteIfExists(file);
            }
            return true;
        }

        // Write the merged rows to a temp file (invisible to the part-* read glob), then swap:
        // delete exactly the files we read, then publish the temp. A file the flusher wrote after
        // we snapshotted `realFiles` (and any skipped corrupt file) is left untouched.
        final Path temp = partitionDir.resolve(COMPACT_TEMP_PREFIX + java.util.UUID.randomUUID() + COMPACT_TEMP_SUFFIX);
        sampleWriter.writeTo(temp, merged);
        for (final Path file : readFiles) {
            Files.deleteIfExists(file);
        }
        moveIntoPlace(temp, SampleWriter.newPartFile(partitionDir));
        return true;
    }

    /** Builds {@code shardDir -> live datasource names} for the shards being purged, from the catalog. */
    private Map<Path, Set<String>> liveNamesForShards(final Set<Path> shardsToPurge) {
        if (shardsToPurge.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Path, Set<String>> liveNamesByShard = new HashMap<>();
        for (final Metric metric : catalog.metrics()) {
            final Tag resourceId = metric.getFirstTagByKey(IntrinsicTagNames.resourceId);
            if (resourceId == null) {
                continue;
            }
            final Path shardDir = pathMapper.shardDir(resourceId.getValue());
            if (shardsToPurge.contains(shardDir)) {
                final Tag name = metric.getFirstTagByKey(IntrinsicTagNames.name);
                liveNamesByShard.computeIfAbsent(shardDir, k -> new HashSet<>())
                        .add(name == null ? "" : name.getValue());
            }
        }
        return liveNamesByShard;
    }

    private List<Path> findPartitionDirs(final Path dataRoot) throws IOException {
        final List<Path> partitions = new ArrayList<>();
        Files.walkFileTree(dataRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                if (dir.equals(dataRoot)) {
                    return FileVisitResult.CONTINUE;
                }
                if (pathMapper.parsePartitionStart(dir.getFileName().toString()).isPresent()) {
                    partitions.add(dir);
                    return FileVisitResult.SKIP_SUBTREE; // partitions don't nest
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return partitions;
    }

    private static List<Path> listPartFiles(final Path partitionDir) throws IOException {
        final String glob = SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX;
        final List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(partitionDir, glob)) {
            for (final Path file : stream) {
                if (Files.isRegularFile(file)) {
                    files.add(file);
                }
            }
        }
        return files;
    }

    private static void moveIntoPlace(final Path temp, final Path target) throws IOException {
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException e) {
            Files.move(temp, target);
        }
    }
}