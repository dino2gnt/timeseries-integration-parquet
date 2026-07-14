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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;

/** Exercises the manual {@code compactNow()} hook behind the {@code tss-parquet:compact} command. */
public class ParquetStorageMaintenanceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String RESOURCE_ID = "snmp/1/eth0/mib2-X-interfaces";
    private static final Instant NOW = Instant.parse("2026-07-06T12:00:00Z");
    private static final Metric METRIC = ImmutableMetric.builder()
            .intrinsicTag("name", "ifHCInOctets")
            .intrinsicTag("resourceId", RESOURCE_ID)
            .build();

    private ParquetStorage storage;

    @After
    public void tearDown() {
        if (storage != null) {
            storage.destroy();
        }
    }

    private ParquetStorage newStorage() throws Exception {
        final ParquetStorage s = new ParquetStorage();
        s.setBaseDir(tempFolder.getRoot().getAbsolutePath());
        s.setFlushInterval("PT1H");     // background flush never fires mid-test
        s.setCompactionInterval("PT1H"); // scheduled compaction never fires mid-test
        s.init();
        return s;
    }

    private List<Sample> samples(final int startOffset, final int count) {
        final List<Sample> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(ImmutableSample.builder().metric(METRIC)
                    .time(NOW.plusSeconds(startOffset + i)).value((double) (startOffset + i)).build());
        }
        return out;
    }

    private int partFileCount() throws Exception {
        final Path partitionDir = new PathMapper(tempFolder.getRoot().toPath())
                .partitionDir(RESOURCE_ID, NOW, Duration.ofDays(1));
        int count = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(partitionDir,
                SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX)) {
            for (final Path ignored : files) {
                count++;
            }
        }
        return count;
    }

    private int readCount() throws Exception {
        final TimeSeriesData data = storage.getTimeSeriesData(ImmutableTimeSeriesFetchRequest.builder()
                .start(NOW.minusSeconds(60)).end(NOW.plusSeconds(600))
                .metric(METRIC).aggregation(Aggregation.NONE).step(Duration.ZERO).build());
        return data.getDataPoints().size();
    }

    @Test
    public void compactNowMergesSmallFilesWithoutLosingRows() throws Exception {
        storage = newStorage();
        storage.store(samples(0, 3));
        storage.flush(); // one part file
        storage.store(samples(3, 3));
        storage.flush(); // a second part file in the same partition
        assertEquals(2, partFileCount());

        final int modified = storage.compactNow();

        assertTrue("at least the one partition was compacted", modified >= 1);
        assertEquals("the two files were merged into one", 1, partFileCount());
        assertEquals("no rows lost in the manual compaction", 6, readCount());
    }

    private int partFileCountAt(final Instant when) throws Exception {
        final Path partitionDir = new PathMapper(tempFolder.getRoot().toPath())
                .partitionDir(RESOURCE_ID, when, Duration.ofDays(1));
        if (!Files.isDirectory(partitionDir)) {
            return 0;
        }
        int count = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(partitionDir,
                SampleWriter.PART_FILE_PREFIX + "*" + SampleWriter.PART_FILE_SUFFIX)) {
            for (final Path ignored : files) {
                count++;
            }
        }
        return count;
    }

    @Test
    public void bulkImportWritesOneFilePerPartitionWithNoCompactionNeeded() throws Exception {
        storage = newStorage();
        // Samples spanning two daily partitions (07-06 and 07-07) for one metric.
        final List<Sample> imported = new ArrayList<>();
        imported.addAll(samples(0, 3));                              // 07-06T12:00:0x
        final Instant nextDay = NOW.plus(Duration.ofDays(1));
        for (int i = 0; i < 4; i++) {
            imported.add(ImmutableSample.builder().metric(METRIC)
                    .time(nextDay.plusSeconds(i)).value((double) (100 + i)).build());
        }

        final int filesWritten = storage.bulkImport(imported);
        storage.flushCatalog();

        assertEquals("one file written per (shard,partition)", 2, filesWritten);
        assertEquals("day 1 partition has exactly one file (no append, no compaction)", 1, partFileCountAt(NOW));
        assertEquals("day 2 partition has exactly one file", 1, partFileCountAt(nextDay));
    }

    @Test
    public void bulkImportedSamplesAreReadableAndMetricIsCatalogued() throws Exception {
        storage = newStorage();
        storage.bulkImport(samples(0, 5));
        storage.flushCatalog();

        assertEquals("all bulk-imported samples are readable", 5, readCount());
        assertEquals("the imported metric is findable via the catalog", 1,
                storage.findMetrics(java.util.List.of(
                        org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher.builder()
                                .type(org.opennms.integration.api.v1.timeseries.TagMatcher.Type.EQUALS)
                                .key("name").value("ifHCInOctets").build())).size());
    }

    @Test
    public void bulkImportIntoAnExistingPartitionAppendsThenCompactionMerges() throws Exception {
        storage = newStorage();
        // First import creates the partition (one file); a second import of the same partition adds
        // a second file (the documented append fallback), which compaction then consolidates.
        storage.bulkImport(samples(0, 3));
        storage.bulkImport(samples(3, 3));
        storage.flushCatalog();
        assertEquals("second import appended a file", 2, partFileCountAt(NOW));

        storage.compactNow();

        assertEquals("compaction merged the appended files", 1, partFileCountAt(NOW));
        assertEquals("no rows lost across import + compaction", 6, readCount());
    }

    @Test
    public void compactNowFailsCleanlyAfterDestroy() throws Exception {
        storage = newStorage();
        storage.destroy();
        try {
            storage.compactNow();
            fail("expected StorageException after destroy()");
        } catch (final StorageException expected) {
            // maintenance executor + compactor are gone; a manual pass must not silently no-op or race
        }
        storage = null; // already destroyed
    }
}