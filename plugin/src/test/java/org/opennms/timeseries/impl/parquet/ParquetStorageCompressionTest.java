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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;

/**
 * Verifies the pure-JVM {@code LZ4_RAW} codec round-trips end to end: values written compressed
 * are read back exactly, exercising the codec's compress and decompress paths on the
 * {@code PlainParquetConfiguration} (no Hadoop {@code Configuration}) code path.
 */
public class ParquetStorageCompressionTest {

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

    @Test
    public void lz4RawRoundTripsValuesExactly() throws Exception {
        storage = new ParquetStorage();
        storage.setBaseDir(tempFolder.getRoot().getAbsolutePath());
        storage.setCompression("LZ4_RAW");
        storage.init();

        final List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            samples.add(ImmutableSample.builder().metric(METRIC)
                    .time(NOW.plusSeconds(i)).value(i * 1.5d).build());
        }
        storage.store(samples);
        storage.flush(); // buffered → force the LZ4_RAW-compressed parquet write

        final TimeSeriesData data = storage.getTimeSeriesData(ImmutableTimeSeriesFetchRequest.builder()
                .start(NOW.minusSeconds(1))
                .end(NOW.plusSeconds(100))
                .metric(METRIC)
                .aggregation(Aggregation.NONE)
                .step(Duration.ZERO)
                .build());

        assertEquals(50, data.getDataPoints().size());
        double expected = 0.0d;
        for (final DataPoint point : data.getDataPoints()) {
            assertEquals(expected, point.getValue(), 0.0d);
            expected += 1.5d;
        }
    }
}