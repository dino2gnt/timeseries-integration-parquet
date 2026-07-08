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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

public class MetricCatalogStoreTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final MetricCatalogStore store = new MetricCatalogStore();
    private Path file;

    @Before
    public void setUp() {
        file = tempFolder.getRoot().toPath().resolve("catalog").resolve("metrics.parquet");
    }

    @Test
    public void readReturnsEmptyWhenSidecarMissing() throws Exception {
        assertTrue(store.read(file).isEmpty());
    }

    @Test
    public void roundTripsAllTagCategoriesIncludingKeylessTags() throws Exception {
        final Metric a = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "snmp/1/eth0/mib2-X-interfaces")
                .metaTag("mtype", Metric.Mtype.counter.name())
                .metaTag("host", "myHost1")
                .externalTag("myExternalTag", "abc-123")
                .externalTag(new org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag("keylessValue"))
                .build();
        final Metric b = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCOutOctets")
                .intrinsicTag("resourceId", "snmp/2/eth1/mib2-X-interfaces")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();

        store.write(file, asList(a, b));
        final List<Metric> loaded = store.read(file);

        assertEquals(2, loaded.size());
        final Map<String, Metric> byKey = new HashMap<>();
        loaded.forEach(m -> byKey.put(m.getKey(), m));

        assertEqualsMetric(a, byKey.get(a.getKey()));
        assertEqualsMetric(b, byKey.get(b.getKey()));
    }

    @Test
    public void writeIsAtomicAndOverwritesPreviousContents() throws Exception {
        final Metric first = ImmutableMetric.builder()
                .intrinsicTag("name", "n1").intrinsicTag("resourceId", "r1").build();
        final Metric second = ImmutableMetric.builder()
                .intrinsicTag("name", "n2").intrinsicTag("resourceId", "r2").build();

        store.write(file, asList(first));
        store.write(file, asList(second)); // full rewrite, not append

        final List<Metric> loaded = store.read(file);
        assertEquals(1, loaded.size());
        assertEquals(second.getKey(), loaded.get(0).getKey());
    }

    private static void assertEqualsMetric(final Metric expected, final Metric actual) {
        // equals() is intrinsic-only, so also assert meta and external are preserved exactly.
        assertEquals(expected, actual);
        assertEquals(setOf(expected.getIntrinsicTags()), setOf(actual.getIntrinsicTags()));
        assertEquals(setOf(expected.getMetaTags()), setOf(actual.getMetaTags()));
        assertEquals(setOf(expected.getExternalTags()), setOf(actual.getExternalTags()));
    }

    private static java.util.Set<Tag> setOf(final java.util.Set<Tag> tags) {
        return new java.util.HashSet<>(tags);
    }
}