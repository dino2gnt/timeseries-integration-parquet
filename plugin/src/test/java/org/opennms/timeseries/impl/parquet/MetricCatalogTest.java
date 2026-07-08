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
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;

public class MetricCatalogTest {

    private MetricCatalog catalog;
    private Metric metric0;
    private Metric metric1;
    private Metric metric2;

    /** Mirrors the shape the storage contract uses: shared name, distinct resourceId per node. */
    private static Metric createMetric(final int nodeId) {
        return ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "nabc") // shared across all metrics
                .intrinsicTag(IntrinsicTagNames.resourceId,
                        String.format("snmp:%d:opennms-jvm:ring_buffer_max_size", nodeId))
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .metaTag("host", "myHost" + nodeId)
                .externalTag("myExternalTag", "external-" + nodeId)
                .build();
    }

    private static TagMatcher matcher(final TagMatcher.Type type, final String key, final String value) {
        return ImmutableTagMatcher.builder().type(type).key(key).value(value).build();
    }

    private String resourceIdOf(final Metric m) {
        return m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue();
    }

    @Before
    public void setUp() {
        catalog = new MetricCatalog();
        metric0 = createMetric(0);
        metric1 = createMetric(1);
        metric2 = createMetric(2);
        catalog.putAll(asList(metric0, metric1, metric2));
    }

    @Test
    public void findsAllMetricsSharingATag() {
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, "nabc")));
        assertEquals(new HashSet<>(asList(metric0, metric1, metric2)), new HashSet<>(found));
    }

    @Test
    public void findsExactlyOneMetricWithUniqueTagCombination() {
        final List<Metric> found = catalog.findMetrics(asList(
                matcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, "nabc"),
                matcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.resourceId, resourceIdOf(metric0))));
        assertEquals(singletonList(metric0), found);
    }

    @Test
    public void findsWithRegexMatching() {
        final String regex = resourceIdOf(metric0).substring(0, 8) + ".*";
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS_REGEX, IntrinsicTagNames.resourceId, regex)));
        assertEquals(singletonList(metric0), found);
    }

    @Test
    public void findsWithNotEquals() {
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.NOT_EQUALS, IntrinsicTagNames.resourceId, resourceIdOf(metric0))));
        assertEquals(new HashSet<>(asList(metric1, metric2)), new HashSet<>(found));
    }

    @Test
    public void findsWithNotEqualsRegex() {
        final String regex = resourceIdOf(metric0).substring(0, 8) + ".*";
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.NOT_EQUALS_REGEX, IntrinsicTagNames.resourceId, regex)));
        assertEquals(new HashSet<>(asList(metric1, metric2)), new HashSet<>(found));
    }

    @Test
    public void searchesMetaTags() {
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, "host", "myHost1")));
        assertEquals(singletonList(metric1), found);
    }

    @Test
    public void doesNotSearchExternalTags() {
        // External tags are collected data and must not be searchable.
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, "myExternalTag", "external-0")));
        assertTrue("external tags must not be searchable", found.isEmpty());
    }

    @Test
    public void everyMatcherMustBeSatisfied() {
        // name matches all, but host matches only metric1 => intersection is metric1.
        final List<Metric> found = catalog.findMetrics(asList(
                matcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, "nabc"),
                matcher(TagMatcher.Type.EQUALS, "host", "myHost1")));
        assertEquals(singletonList(metric1), found);
    }

    @Test
    public void findRejectsNullAndEmptyMatchers() {
        assertThrows(NullPointerException.class, () -> catalog.findMetrics(null));
        assertThrows(IllegalArgumentException.class, () -> catalog.findMetrics(new HashSet<>()));
    }

    @Test
    public void putIsUpsertKeyedOnIntrinsicIdentity() {
        assertEquals(3, catalog.size());
        // Re-store a metric with the same intrinsic tags but different meta => no duplicate,
        // and the refreshed meta tag becomes searchable.
        final Metric refreshed = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "nabc")
                .intrinsicTag(IntrinsicTagNames.resourceId, resourceIdOf(metric0))
                .metaTag("host", "renamedHost")
                .build();
        catalog.put(refreshed);
        assertEquals(3, catalog.size());
        assertEquals(singletonList(refreshed), catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, "host", "renamedHost"))));
        assertTrue(catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, "host", "myHost0"))).isEmpty());
    }

    @Test
    public void removeIsLogicalDelete() {
        catalog.remove(metric0);
        assertEquals(2, catalog.size());
        final List<Metric> found = catalog.findMetrics(singletonList(
                matcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.resourceId, resourceIdOf(metric0))));
        assertTrue(found.isEmpty());
    }
}