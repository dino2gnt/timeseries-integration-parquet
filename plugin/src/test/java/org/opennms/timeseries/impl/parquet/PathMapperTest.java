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
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

public class PathMapperTest {

    private final Path baseDir = Paths.get("/base");
    private final PathMapper mapper = new PathMapper(baseDir);

    @Test
    public void dataRootIsUnderBaseDir() {
        assertEquals(Paths.get("/base", "data"), mapper.dataRoot());
    }

    @Test
    public void mapsStoreByForeignSourceResourceIdToShardDir() {
        // storeByForeignSource layout: snmp/fs/<foreignSource>/<foreignId>/<ifName>/<group>
        final Path dir = mapper.shardDir("snmp/fs/Servers/node1/eth0/mib2-X-interfaces");
        assertEquals(Paths.get("/base", "data", "snmp", "fs", "Servers", "node1", "eth0", "mib2-X-interfaces"), dir);
    }

    @Test
    public void mapsStoreByNodeIdResourceIdToShardDir() {
        // default layout: snmp/<nodeId>/<ifName>/<group>
        final Path dir = mapper.shardDir("snmp/1/eth0/mib2-X-interfaces");
        assertEquals(Paths.get("/base", "data", "snmp", "1", "eth0", "mib2-X-interfaces"), dir);
    }

    @Test
    public void percentEncodesUnsafeCharactersPerElement() {
        // The contract's resourceId is colon-delimited with '=' and no '/', so it is one element.
        final Path dir = mapper.shardDir("snmp:0:opennms-jvm:name=unknown");
        assertEquals(Paths.get("/base", "data", "snmp%3A0%3Aopennms-jvm%3Aname%3Dunknown"), dir);
    }

    @Test
    public void preservesUnreservedCharactersIncludingDotsInsideElements() {
        assertEquals("mib2.X_interfaces-1", PathMapper.sanitize("mib2.X_interfaces-1"));
    }

    @Test
    public void guardsAgainstPathTraversalElements() {
        // A bare "." or ".." element must never resolve to a real relative path.
        assertEquals("%2E", PathMapper.sanitize("."));
        assertEquals("%2E%2E", PathMapper.sanitize(".."));
        final Path dir = mapper.shardDir("snmp/../etc/passwd");
        assertEquals(Paths.get("/base", "data", "snmp", "%2E%2E", "etc", "passwd"), dir);
        assertTrue("shard dir must stay under the data root", dir.startsWith(mapper.dataRoot()));
    }

    @Test
    public void collapsesEmptyPathElementsFromExtraSlashes() {
        assertEquals(Paths.get("/base", "data", "snmp", "1"), mapper.shardDir("/snmp//1/"));
    }

    @Test
    public void shardDirRejectsNullAndEmpty() {
        assertThrows(NullPointerException.class, () -> mapper.shardDir(null));
        assertThrows(IllegalArgumentException.class, () -> mapper.shardDir(""));
    }

    @Test
    public void dailyPartitionNameUsesDateOnly() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        assertEquals("p=2026-07-06", mapper.partitionName(t, Duration.ofDays(1)));
    }

    @Test
    public void dailyPartitionFloorsToStartAndEndOfDay() {
        assertEquals("p=2026-07-06", mapper.partitionName(Instant.parse("2026-07-06T00:00:00Z"), Duration.ofDays(1)));
        assertEquals("p=2026-07-06", mapper.partitionName(Instant.parse("2026-07-06T23:59:59Z"), Duration.ofDays(1)));
    }

    @Test
    public void hourlyPartitionNameIncludesHour() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        assertEquals("p=2026-07-06-14", mapper.partitionName(t, Duration.ofHours(1)));
    }

    @Test
    public void subHourlyPartitionFloorsToBoundaryWithMinutePrecision() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        assertEquals("p=2026-07-06-14-15", mapper.partitionName(t, Duration.ofMinutes(15)));
    }

    @Test
    public void partitionDirComposesShardAndPartition() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        final Path dir = mapper.partitionDir("snmp/1/eth0/g", t, Duration.ofDays(1));
        assertEquals(Paths.get("/base", "data", "snmp", "1", "eth0", "g", "p=2026-07-06"), dir);
    }

    @Test
    public void partitionNameRejectsNonPositiveDuration() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        assertThrows(IllegalArgumentException.class, () -> mapper.partitionName(t, Duration.ZERO));
    }

    @Test
    public void parsePartitionStartInvertsPartitionNameForEachGranularity() {
        assertEquals(Instant.parse("2026-07-06T00:00:00Z"), mapper.parsePartitionStart("p=2026-07-06").orElseThrow());
        assertEquals(Instant.parse("2026-07-06T14:00:00Z"), mapper.parsePartitionStart("p=2026-07-06-14").orElseThrow());
        assertEquals(Instant.parse("2026-07-06T14:15:00Z"), mapper.parsePartitionStart("p=2026-07-06-14-15").orElseThrow());
    }

    @Test
    public void parsePartitionStartRoundTripsPartitionName() {
        final Instant t = Instant.parse("2026-07-06T14:23:45Z");
        final String name = mapper.partitionName(t, Duration.ofHours(1));
        assertEquals(Instant.parse("2026-07-06T14:00:00Z"), mapper.parsePartitionStart(name).orElseThrow());
    }

    @Test
    public void parsePartitionStartReturnsEmptyForNonPartitionNames() {
        assertTrue(mapper.parsePartitionStart(null).isEmpty());
        assertTrue(mapper.parsePartitionStart("data").isEmpty());
        assertTrue(mapper.parsePartitionStart("p=not-a-date").isEmpty());
        assertTrue(mapper.parsePartitionStart("p=2026-13-40").isEmpty()); // invalid month/day
    }
}