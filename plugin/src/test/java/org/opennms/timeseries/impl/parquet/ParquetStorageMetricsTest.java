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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;

/** Verifies the plugin's operational metrics are exported over JMX and track the real operations. */
public class ParquetStorageMetricsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Instant NOW = Instant.parse("2026-07-06T12:00:00Z");
    private static final Metric METRIC = ImmutableMetric.builder()
            .intrinsicTag("name", "ifHCInOctets")
            .intrinsicTag("resourceId", "snmp/1/eth0/mib2-X-interfaces")
            .build();

    private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
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
        s.setFlushInterval("PT1H");
        s.setCompactionInterval("PT1H");
        s.init();
        return s;
    }

    private static List<Sample> samples(final int start, final int count) {
        final List<Sample> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(ImmutableSample.builder().metric(METRIC).time(NOW.plusSeconds(start + i)).value((double) (start + i)).build());
        }
        return out;
    }

    private void read() throws Exception {
        storage.getTimeSeriesData(ImmutableTimeSeriesFetchRequest.builder()
                .start(NOW.minusSeconds(60)).end(NOW.plusSeconds(600))
                .metric(METRIC).aggregation(Aggregation.NONE).step(Duration.ZERO).build());
    }

    private long count(final String metricName) throws Exception {
        final Set<ObjectName> found = mbs.queryNames(
                new ObjectName(ParquetMetrics.JMX_DOMAIN + ":name=" + metricName + ",*"), null);
        assertEquals("exactly one MBean for " + metricName, 1, found.size());
        return (Long) mbs.getAttribute(found.iterator().next(), "Count");
    }

    @Test
    public void allMetricsAreRegisteredUnderTheJmxDomain() throws Exception {
        storage = newStorage();
        final Set<ObjectName> names = mbs.queryNames(new ObjectName(ParquetMetrics.JMX_DOMAIN + ":*"), null);
        final List<String> metricNames = names.stream().map(n -> n.getKeyProperty("name")).sorted().toList();
        assertTrue(metricNames.contains("compaction"));
        assertTrue(metricNames.contains("samples.written"));
        assertTrue(metricNames.contains("samples.read"));
        assertTrue(metricNames.contains("partitions.created"));
        assertTrue(metricNames.contains("partitions.deleted"));
        assertTrue(metricNames.contains("partitions.compacted"));
    }

    @Test
    public void countersAndTimerTrackWritesReadsAndCompaction() throws Exception {
        storage = newStorage();

        storage.store(samples(0, 3));
        storage.flush();
        assertEquals("3 samples written", 3L, count("samples.written"));
        assertEquals("one new partition created", 1L, count("partitions.created"));

        read();
        assertEquals("3 samples read back from disk", 3L, count("samples.read"));

        // A second file in the same partition, then a manual compaction pass.
        storage.store(samples(3, 3));
        storage.flush();
        storage.compactNow();
        assertEquals("6 samples written total", 6L, count("samples.written"));
        assertEquals("still one partition (same day, not a new one)", 1L, count("partitions.created"));
        assertTrue("compaction timer recorded at least one pass", count("compaction") >= 1);
        assertTrue("at least one partition compacted", count("partitions.compacted") >= 1);
    }

    @Test
    public void mbeansAreUnregisteredOnDestroy() throws Exception {
        storage = newStorage();
        assertTrue(mbs.queryNames(new ObjectName(ParquetMetrics.JMX_DOMAIN + ":*"), null).size() >= 6);
        storage.destroy();
        storage = null;
        assertTrue("JMX metrics unregistered after destroy",
                mbs.queryNames(new ObjectName(ParquetMetrics.JMX_DOMAIN + ":*"), null).isEmpty());
    }
}