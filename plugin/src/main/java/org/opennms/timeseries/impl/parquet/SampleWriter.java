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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.UUID;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

/**
 * Writes a batch of samples to a new, immutable {@code part-*.parquet} file inside a partition
 * directory.
 *
 * <p>Each call to {@link #write} appends one new file with a unique name, so concurrent writes to
 * the same partition never collide and reads simply enumerate every {@code part-*} file. The
 * writer stays on the pure-JVM path: a {@link LocalOutputFile} plus a
 * {@link PlainParquetConfiguration} avoid instantiating a Hadoop {@code Configuration}, and the
 * {@code UNCOMPRESSED} codec avoids the Hadoop codec {@code ServiceLoader} entirely (compression
 * is made configurable in a later milestone).</p>
 */
class SampleWriter {

    static final String PART_FILE_PREFIX = "part-";
    static final String PART_FILE_SUFFIX = ".parquet";
    /** Hidden temp for an in-progress write; leading dot + {@code .tmp} keeps it out of the part glob. */
    static final String TEMP_PREFIX = ".";
    static final String TEMP_SUFFIX = ".tmp";

    private final CompressionCodecName compression;

    SampleWriter() {
        this(CompressionCodecName.UNCOMPRESSED);
    }

    SampleWriter(final CompressionCodecName compression) {
        this.compression = compression;
    }

    /**
     * Writes every {@code (time, name, value)} row for the given samples to a fresh parquet file
     * in {@code partitionDir}, creating the directory (and its parents) if necessary.
     *
     * <p>The write is atomic from a reader's point of view: rows are written to a hidden temp file
     * that does <em>not</em> match the {@code part-*.parquet} glob, then atomically moved into place.
     * Readers and the compactor only ever enumerate {@code part-*.parquet}, so they never observe a
     * half-written or 0-byte file. A crash mid-write leaves only the temp (invisible to those globs
     * and cleaned up on the next successful pass), never a broken {@code part-*.parquet}.</p>
     *
     * @return the path of the file that was written
     */
    Path write(final Path partitionDir, final Collection<SampleRow> rows) throws IOException {
        Files.createDirectories(partitionDir);
        final Path file = newPartFile(partitionDir);
        final Path temp = partitionDir.resolve(TEMP_PREFIX + file.getFileName() + TEMP_SUFFIX);
        try {
            writeTo(temp, rows);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp); // no-op once the move has succeeded
        }
        return file;
    }

    /** A fresh, unique {@code part-*.parquet} path within a partition directory. */
    static Path newPartFile(final Path partitionDir) {
        return partitionDir.resolve(PART_FILE_PREFIX + UUID.randomUUID() + PART_FILE_SUFFIX);
    }

    /** Writes the rows to an exact file path (used both for new part files and compaction temps). */
    void writeTo(final Path file, final Collection<SampleRow> rows) throws IOException {
        // Supply our own pure-JVM codec factory so parquet never instantiates a codec through a
        // Hadoop Configuration (which would drag in woodstox and the rest of Hadoop's excluded
        // transitives). PlainParquetConfiguration keeps the write path Hadoop-Configuration-free.
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(SampleSchema.SCHEMA)
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(compression)
                .withCodecFactory(new AircompressorCodecFactory())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            for (final SampleRow row : rows) {
                final Group group = new SimpleGroup(SampleSchema.SCHEMA);
                group.add(SampleSchema.IDX_TIME, SampleSchema.toEpochMicros(row.time()));
                group.add(SampleSchema.IDX_NAME, row.name());
                group.add(SampleSchema.IDX_VALUE, row.value());
                writer.write(group);
            }
        }
    }
}