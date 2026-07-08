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
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;

/**
 * Verifies that the durable catalog sidecar lets metric definitions (including the meta and
 * external tags that live only in the catalog, never in the sample files) survive a restart.
 */
public class ParquetStorageRestartTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Metric METRIC = ImmutableMetric.builder()
            .intrinsicTag("name", "ifHCInOctets")
            .intrinsicTag("resourceId", "snmp/1/eth0/mib2-X-interfaces")
            .metaTag("mtype", Metric.Mtype.counter.name())
            .metaTag("host", "myHost1")
            .externalTag("collectedBy", "minion-a")
            .build();

    private static TagMatcher nameEquals() {
        return ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key(IntrinsicTagNames.name)
                .value("ifHCInOctets")
                .build();
    }

    private ParquetStorage open(final Path baseDir) throws Exception {
        final ParquetStorage storage = new ParquetStorage(baseDir);
        storage.init();
        return storage;
    }

    @Test
    public void metricWithMetaAndExternalTagsSurvivesRestart() throws Exception {
        final Path baseDir = tempFolder.getRoot().toPath();

        final ParquetStorage first = open(baseDir);
        first.store(singletonList(ImmutableSample.builder()
                .metric(METRIC).time(Instant.parse("2026-07-06T12:00:00Z")).value(1.0d).build()));
        first.destroy();

        // A brand-new instance on the same baseDir, WITHOUT storing anything, must recover the
        // full metric from the sidecar.
        final ParquetStorage restarted = open(baseDir);
        try {
            final List<Metric> found = restarted.findMetrics(singletonList(nameEquals()));
            assertEquals(1, found.size());
            final Metric recovered = found.get(0);
            assertEquals(METRIC, recovered);
            assertEquals("meta tags must survive restart", METRIC.getMetaTags(), recovered.getMetaTags());
            assertEquals("external tags must survive restart", METRIC.getExternalTags(), recovered.getExternalTags());
        } finally {
            restarted.destroy();
        }
    }

    @Test
    public void logicalDeleteSurvivesRestart() throws Exception {
        final Path baseDir = tempFolder.getRoot().toPath();

        final ParquetStorage first = open(baseDir);
        first.store(singletonList(ImmutableSample.builder()
                .metric(METRIC).time(Instant.parse("2026-07-06T12:00:00Z")).value(1.0d).build()));
        first.delete(METRIC);
        first.destroy();

        final ParquetStorage restarted = open(baseDir);
        try {
            assertTrue("a deleted metric must not reappear after restart",
                    restarted.findMetrics(singletonList(nameEquals())).isEmpty());
        } finally {
            restarted.destroy();
        }
    }
}