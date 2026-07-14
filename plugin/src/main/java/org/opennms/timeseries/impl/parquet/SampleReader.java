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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads samples for a single datasource {@code name} out of a shard directory, filtering by time
 * range.
 *
 * <p>A shard holds one {@code p=<partition>} directory per time partition, each containing one or
 * more immutable {@code part-*.parquet} files. This reader enumerates every partition file under
 * the shard and returns the rows whose {@code name} matches and whose timestamp falls strictly
 * within {@code (start, end)} &mdash; the same open interval the reference {@code InMemoryStorage}
 * uses. Partition-level pruning by the {@code p=} directory name is a future optimisation; for now
 * correctness comes from filtering every row.</p>
 *
 * <p>Reads stay on the pure-JVM path via {@link LocalInputFile}, a {@link PlainParquetConfiguration}
 * and an {@link AircompressorCodecFactory} for decompression (so no Hadoop {@code Configuration} is
 * touched); the {@link ParquetReader.Builder} is subclassed only to supply a {@link GroupReadSupport}
 * (the {@code read(InputFile)} factory leaves it null).</p>
 */
class SampleReader {

    private static final Logger LOG = LoggerFactory.getLogger(SampleReader.class);

    /**
     * @param shardDir the shard directory for a resourceId (may not exist yet)
     * @param name     the datasource name to match
     * @param start    exclusive lower time bound
     * @param end      exclusive upper time bound
     * @return the matching rows, in file/scan order (the caller sorts)
     */
    List<SampleRow> read(final Path shardDir, final String name, final Instant start, final Instant end)
            throws IOException {
        final List<SampleRow> rows = new ArrayList<>();
        if (!Files.isDirectory(shardDir)) {
            return rows;
        }
        try (DirectoryStream<Path> partitions =
                     Files.newDirectoryStream(shardDir, PathMapper.PARTITION_PREFIX + "*")) {
            for (final Path partitionDir : partitions) {
                if (Files.isDirectory(partitionDir)) {
                    readPartition(partitionDir, name, start, end, rows);
                }
            }
        }
        return rows;
    }

    private void readPartition(final Path partitionDir, final String name, final Instant start,
                               final Instant end, final List<SampleRow> out) throws IOException {
        final String glob = SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(partitionDir, glob)) {
            for (final Path file : files) {
                if (!Files.isRegularFile(file)) {
                    continue;
                }
                if (Files.size(file) == 0) {
                    // A 0-byte part file is a crash/interrupted-write leftover with no rows. Skip it
                    // rather than let parquet throw "not a Parquet file" and fail the whole query;
                    // compaction removes these.
                    LOG.warn("Skipping empty parquet part file {}.", file);
                    continue;
                }
                try {
                    readFile(file, name, start, end, out);
                } catch (final RuntimeException e) {
                    // One corrupt file must not blank out every graph for this resource. Skip it and
                    // keep reading the rest of the shard.
                    LOG.warn("Skipping unreadable parquet part file {}.", file, e);
                }
            }
        }
    }

    /** Reads every row of a single parquet file, unfiltered. Used by compaction to merge files. */
    List<SampleRow> readAllRows(final Path file) throws IOException {
        final List<SampleRow> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = new GroupReaderBuilder(
                new LocalInputFile(file), new PlainParquetConfiguration())
                .withCodecFactory(new AircompressorCodecFactory()).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                rows.add(new SampleRow(
                        SampleSchema.fromEpochMicros(group.getLong(SampleSchema.IDX_TIME, 0)),
                        group.getString(SampleSchema.IDX_NAME, 0),
                        group.getDouble(SampleSchema.IDX_VALUE, 0)));
            }
        }
        return rows;
    }

    private void readFile(final Path file, final String name, final Instant start, final Instant end,
                          final List<SampleRow> out) throws IOException {
        try (ParquetReader<Group> reader = new GroupReaderBuilder(
                new LocalInputFile(file), new PlainParquetConfiguration())
                .withCodecFactory(new AircompressorCodecFactory()).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                final String rowName = group.getString(SampleSchema.IDX_NAME, 0);
                if (!name.equals(rowName)) {
                    continue;
                }
                final Instant time = SampleSchema.fromEpochMicros(group.getLong(SampleSchema.IDX_TIME, 0));
                if (time.isAfter(start) && time.isBefore(end)) {
                    out.add(new SampleRow(time, rowName, group.getDouble(SampleSchema.IDX_VALUE, 0)));
                }
            }
        }
    }

    /**
     * A {@link ParquetReader.Builder} that reads the example {@link Group} object model from an
     * {@link InputFile}. The stock {@code read(InputFile)} builder leaves the read support null,
     * so we override {@link #getReadSupport()} to supply a {@link GroupReadSupport}.
     */
    private static final class GroupReaderBuilder extends ParquetReader.Builder<Group> {
        private GroupReaderBuilder(final InputFile file, final ParquetConfiguration conf) {
            super(file, conf);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }
}