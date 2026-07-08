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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

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
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;

/**
 * Exercises the asynchronous flush buffer: samples are held in memory until an explicit flush, the
 * row threshold, or shutdown writes them to parquet.
 */
public class ParquetStorageBufferTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Instant NOW = Instant.parse("2026-07-06T12:00:00Z");
    private static final Metric METRIC = ImmutableMetric.builder()
            .intrinsicTag("name", "ifHCInOctets")
            .intrinsicTag("resourceId", "snmp/1/eth0/mib2-X-interfaces")
            .build();

    private ParquetStorage storage;

    @After
    public void tearDown() {
        if (storage != null) {
            storage.destroy();
        }
    }

    /** A storage whose background flush never fires during the test, isolating the behaviour under test. */
    private ParquetStorage newStorage(final String maxRows) throws Exception {
        final ParquetStorage s = new ParquetStorage();
        s.setBaseDir(tempFolder.getRoot().getAbsolutePath());
        s.setFlushMaxRows(maxRows);
        s.setFlushInterval("PT1H");
        s.init();
        return s;
    }

    private static List<Sample> samples(final int count) {
        final List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            samples.add(ImmutableSample.builder().metric(METRIC).time(NOW.plusSeconds(i)).value((double) i).build());
        }
        return samples;
    }

    private int readCount() throws Exception {
        final TimeSeriesData data = storage.getTimeSeriesData(ImmutableTimeSeriesFetchRequest.builder()
                .start(NOW.minusSeconds(60))
                .end(NOW.plusSeconds(600))
                .metric(METRIC)
                .aggregation(Aggregation.NONE)
                .step(Duration.ZERO)
                .build());
        return data.getDataPoints().size();
    }

    @Test
    public void bufferedSamplesAreNotOnDiskUntilFlushed() throws Exception {
        storage = newStorage("8192"); // high threshold: nothing auto-flushes
        storage.store(samples(3));

        assertEquals("buffered writes are not yet readable from disk", 0, readCount());

        storage.flush();
        assertEquals("flush writes the buffered samples", 3, readCount());
    }

    @Test
    public void reachingTheRowThresholdFlushesEagerly() throws Exception {
        storage = newStorage("1"); // every row crosses the threshold
        storage.store(samples(3));

        // No explicit flush: crossing the threshold during store() already drained the bucket.
        assertEquals(3, readCount());
    }

    @Test
    public void destroyFlushesBufferedSamples() throws Exception {
        final java.nio.file.Path baseDir = tempFolder.getRoot().toPath();
        final ParquetStorage first = new ParquetStorage(baseDir);
        first.init();
        first.store(singletonList(
                ImmutableSample.builder().metric(METRIC).time(NOW).value(7.0d).build()));
        first.destroy(); // must drain the buffer to disk

        storage = new ParquetStorage(baseDir);
        storage.init();
        assertEquals("samples buffered at shutdown are persisted by destroy()", 1, readCount());
    }
}