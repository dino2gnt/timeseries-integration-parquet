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