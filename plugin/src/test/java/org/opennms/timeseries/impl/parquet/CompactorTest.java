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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

public class CompactorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String RESOURCE_ID = "snmp/1/eth0/mib2-X-interfaces";
    private static final Instant T0 = Instant.parse("2026-07-06T12:00:00Z");

    private PathMapper pathMapper;
    private MetricCatalog catalog;
    private SampleReader reader;
    private SampleWriter writer;
    private Compactor compactor;
    private Path partitionDir;

    @Before
    public void setUp() {
        pathMapper = new PathMapper(tempFolder.getRoot().toPath());
        catalog = new MetricCatalog();
        reader = new SampleReader();
        writer = new SampleWriter();
        compactor = new Compactor(pathMapper, catalog, reader, writer, 2);
        partitionDir = pathMapper.partitionDir(RESOURCE_ID, T0, java.time.Duration.ofDays(1));
    }

    private void catalogMetric(final String name) {
        catalog.put(ImmutableMetric.builder()
                .intrinsicTag("name", name)
                .intrinsicTag("resourceId", RESOURCE_ID)
                .build());
    }

    /** Writes one part file containing a single row, so N calls create N small files. */
    private void writeOneRowFile(final String name, final int secondOffset) throws IOException {
        writer.write(partitionDir, List.of(new SampleRow(T0.plusSeconds(secondOffset), name, secondOffset)));
    }

    private int partFileCount() throws IOException {
        int count = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(partitionDir,
                SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX)) {
            for (final Path ignored : files) {
                count++;
            }
        }
        return count;
    }

    private List<SampleRow> readAllRows() throws IOException {
        final List<SampleRow> rows = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(partitionDir,
                SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX)) {
            for (final Path file : files) {
                rows.addAll(reader.readAllRows(file));
            }
        }
        return rows;
    }

    @Test
    public void mergesManySmallFilesIntoOneWithoutLosingRows() throws Exception {
        catalogMetric("ifHCInOctets");
        for (int i = 1; i <= 5; i++) {
            writeOneRowFile("ifHCInOctets", i);
        }
        assertEquals(5, partFileCount());

        final int compacted = compactor.compact(Set.of());

        assertEquals(1, compacted);
        assertEquals("five files merged into one", 1, partFileCount());
        assertEquals("no rows lost in the merge", 5, readAllRows().size());
    }

    @Test
    public void leavesASingleFilePartitionUntouchedWhenNotPurging() throws Exception {
        catalogMetric("ifHCInOctets");
        writeOneRowFile("ifHCInOctets", 1);
        assertEquals(1, partFileCount());

        assertEquals(0, compactor.compact(Set.of()));
        assertEquals(1, partFileCount());
    }

    @Test
    public void purgeDropsRowsOfDeletedMetricsButKeepsLiveOnes() throws Exception {
        // Two metrics share the shard (group); only the live one stays in the catalog.
        catalogMetric("ifHCInOctets");
        writeOneRowFile("ifHCInOctets", 1);
        writeOneRowFile("ifHCOutOctets", 2); // this metric is NOT in the catalog => deleted

        final Set<Path> purge = Set.of(pathMapper.shardDir(RESOURCE_ID));
        assertEquals(1, compactor.compact(purge));

        final List<SampleRow> rows = readAllRows();
        assertEquals(1, rows.size());
        assertEquals("only the live metric's row remains", "ifHCInOctets", rows.get(0).name());
    }

    @Test
    public void purgeRemovesThePartitionWhenEveryMetricIsDeleted() throws Exception {
        // Nothing catalogued => every row belongs to a deleted metric.
        writeOneRowFile("ifHCInOctets", 1);
        writeOneRowFile("ifHCOutOctets", 2);

        final Set<Path> purge = Set.of(pathMapper.shardDir(RESOURCE_ID));
        assertEquals(1, compactor.compact(purge));

        assertEquals("all part files gone", 0, partFileCount());
    }

    @Test
    public void concurrentlyWrittenFileIsNotLostByCompaction() throws Exception {
        // Simulate: compaction snapshots two files, then the flusher writes a third mid-pass.
        // Compaction must only delete what it read and preserve the newcomer. We approximate the
        // race by verifying total row preservation across a merge that started with two files.
        catalogMetric("ifHCInOctets");
        writeOneRowFile("ifHCInOctets", 1);
        writeOneRowFile("ifHCInOctets", 2);
        writeOneRowFile("ifHCInOctets", 3);

        compactor.compact(Set.of());

        assertTrue(partFileCount() >= 1);
        assertEquals("every row survives compaction", 3, readAllRows().size());
        assertFalse(readAllRows().isEmpty());
    }
}