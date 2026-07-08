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
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
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

public class ParquetStorageConfigTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ParquetStorage storage;

    @After
    public void tearDown() {
        if (storage != null) {
            storage.destroy();
        }
    }

    @Test
    public void initFailsFastWithoutBaseDirOrSystemProperty() {
        final String previous = System.getProperty(ParquetStorage.RRD_BASE_DIR_PROPERTY);
        System.clearProperty(ParquetStorage.RRD_BASE_DIR_PROPERTY);
        try {
            storage = new ParquetStorage();
            storage.setBaseDir("   "); // blank => ignored, so still no baseDir
            assertThrows(StorageException.class, storage::init);
        } finally {
            if (previous != null) {
                System.setProperty(ParquetStorage.RRD_BASE_DIR_PROPERTY, previous);
            }
        }
    }

    @Test
    public void appliesStringConfigurationAndRoundTripsASample() throws Exception {
        storage = new ParquetStorage();
        storage.setBaseDir(tempFolder.getRoot().getAbsolutePath());
        storage.setPartitionDuration("PT1H");
        storage.setRetention("P30D");
        storage.setRetentionSweepInterval("PT6H");
        storage.setCompression("uncompressed"); // case-insensitive
        storage.setPruneCatalogOnExpiry(true);
        storage.init();

        final Instant now = Instant.parse("2026-07-06T12:00:00Z");
        final Metric metric = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "snmp/1/eth0/mib2-X-interfaces")
                .metaTag("mtype", Metric.Mtype.counter.name())
                .build();
        final Sample sample = ImmutableSample.builder().metric(metric).time(now).value(1.5d).build();
        storage.store(List.of(sample));
        storage.flush(); // sample writes are buffered; make them observable before reading back

        final TimeSeriesData data = storage.getTimeSeriesData(ImmutableTimeSeriesFetchRequest.builder()
                .start(now.minusSeconds(60))
                .end(now.plusSeconds(60))
                .metric(metric)
                .aggregation(Aggregation.NONE)
                .step(Duration.ZERO)
                .build());

        assertEquals(1, data.getDataPoints().size());
        assertEquals(1.5d, data.getDataPoints().get(0).getValue(), 0.0d);
    }

    @Test
    public void setCompressionRejectsUnknownCodec() {
        storage = new ParquetStorage();
        assertThrows(IllegalArgumentException.class, () -> storage.setCompression("no-such-codec"));
    }
}