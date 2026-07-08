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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

public class RetentionManagerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // Fixed "now" so expiry decisions are deterministic.
    private static final Instant NOW = Instant.parse("2026-07-06T00:00:00Z");
    private static final Duration RETENTION = Duration.ofDays(30); // cutoff = 2026-06-06T00:00:00Z
    private static final Duration PARTITION = Duration.ofDays(1);

    private PathMapper pathMapper;
    private MetricCatalog catalog;
    private RetentionManager retention;

    @Before
    public void setUp() {
        pathMapper = new PathMapper(tempFolder.getRoot().toPath());
        catalog = new MetricCatalog();
        retention = new RetentionManager(pathMapper, catalog, RETENTION, PARTITION, true, () -> NOW);
    }

    private static Metric metric(final String resourceId) {
        return ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "ifHCInOctets")
                .intrinsicTag(IntrinsicTagNames.resourceId, resourceId)
                .build();
    }

    /** Creates a {@code <shard>/p=<day>} directory containing a dummy part file. */
    private Path partition(final String resourceId, final String day) throws IOException {
        final Path dir = pathMapper.shardDir(resourceId).resolve(PathMapper.PARTITION_PREFIX + day);
        Files.createDirectories(dir);
        Files.createFile(dir.resolve("part-dummy.parquet"));
        return dir;
    }

    @Test
    public void deletesPartitionsOlderThanRetentionAndKeepsRecentOnes() throws IOException {
        final Path old = partition("shardA", "2026-05-01"); // window ends 2026-05-02 < cutoff => expired
        final Path recent = partition("shardA", "2026-07-05"); // window ends 2026-07-06 > cutoff => kept
        catalog.put(metric("shardA"));

        final int deleted = retention.sweep();

        assertEquals(1, deleted);
        assertFalse("expired partition should be gone", Files.exists(old));
        assertTrue("recent partition should remain", Files.exists(recent));
        assertEquals("metric still has a partition, so it stays catalogued", 1, catalog.size());
    }

    @Test
    public void expiresPartitionWhoseWindowEndsExactlyAtCutoff() throws IOException {
        // p=2026-06-05 spans [06-05, 06-06); its end equals the cutoff (2026-06-06) => expired.
        final Path boundary = partition("shardB", "2026-06-05");
        catalog.put(metric("shardB"));

        assertEquals(1, retention.sweep());
        assertFalse(Files.exists(boundary));
    }

    @Test
    public void removesEmptiedShardDirectoriesAndPrunesTheirCatalogEntries() throws IOException {
        partition("shardGone", "2026-01-01"); // only partition, expired => shard empties out
        partition("shardKept", "2026-07-05"); // recent => shard survives
        catalog.put(metric("shardGone"));
        catalog.put(metric("shardKept"));

        final int deleted = retention.sweep();

        assertEquals(1, deleted);
        assertFalse("emptied shard directory should be removed",
                Files.exists(pathMapper.shardDir("shardGone")));
        assertTrue(Files.exists(pathMapper.shardDir("shardKept")));
        assertEquals(1, catalog.size());
        assertTrue(catalog.get(metric("shardKept").getKey()) != null);
        assertTrue("metric with no remaining partitions is pruned",
                catalog.get(metric("shardGone").getKey()) == null);
    }

    @Test
    public void doesNotPruneCatalogWhenPruningDisabled() throws IOException {
        partition("shardGone", "2026-01-01"); // expired => shard empties out
        catalog.put(metric("shardGone"));

        final RetentionManager noPrune =
                new RetentionManager(pathMapper, catalog, RETENTION, PARTITION, false, () -> NOW);
        assertEquals(1, noPrune.sweep());

        assertFalse("partition data is still deleted", Files.exists(pathMapper.shardDir("shardGone")));
        assertEquals("but the catalog entry is retained when pruning is disabled", 1, catalog.size());
    }

    @Test
    public void ignoresNonPartitionDirectoriesAndToleratesMissingDataRoot() throws IOException {
        // No data written yet: sweep is a no-op and must not fail.
        assertEquals(0, retention.sweep());

        // A non-partition directory that still holds content must never be treated as an
        // expired partition (only genuinely empty directories are cleaned up).
        final Path stray = pathMapper.shardDir("shardC").resolve("not-a-partition");
        Files.createDirectories(stray);
        Files.createFile(stray.resolve("keep.me"));
        assertEquals(0, retention.sweep());
        assertTrue(Files.exists(stray));
    }
}