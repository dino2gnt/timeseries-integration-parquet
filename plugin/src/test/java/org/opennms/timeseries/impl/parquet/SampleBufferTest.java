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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class SampleBufferTest {

    private static final Path DIR_A = Paths.get("/data/shardA/p=2026-07-06");
    private static final Path DIR_B = Paths.get("/data/shardB/p=2026-07-06");

    private static SampleRow row(final int i) {
        return new SampleRow(Instant.parse("2026-07-06T00:00:00Z").plusSeconds(i), "n", (double) i);
    }

    @Test
    public void addReportsThresholdCrossing() {
        final SampleBuffer buffer = new SampleBuffer(3);
        assertFalse(buffer.add(DIR_A, row(1)));
        assertFalse(buffer.add(DIR_A, row(2)));
        assertTrue("third row reaches the threshold of 3", buffer.add(DIR_A, row(3)));
    }

    @Test
    public void thresholdIsPerBucket() {
        final SampleBuffer buffer = new SampleBuffer(2);
        assertFalse(buffer.add(DIR_A, row(1)));
        assertFalse("a different bucket has its own count", buffer.add(DIR_B, row(1)));
        assertTrue(buffer.add(DIR_A, row(2)));
    }

    @Test
    public void drainBucketRemovesAndReturnsRows() {
        final SampleBuffer buffer = new SampleBuffer(100);
        buffer.add(DIR_A, row(1));
        buffer.add(DIR_A, row(2));

        final List<SampleRow> drained = buffer.drainBucket(DIR_A);
        assertEquals(2, drained.size());
        assertNull("bucket is gone after draining", buffer.drainBucket(DIR_A));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void drainAllEmptiesEveryBucket() {
        final SampleBuffer buffer = new SampleBuffer(100);
        buffer.add(DIR_A, row(1));
        buffer.add(DIR_B, row(1));
        buffer.add(DIR_B, row(2));

        final Map<Path, List<SampleRow>> drained = buffer.drainAll();
        assertEquals(2, drained.size());
        assertEquals(1, drained.get(DIR_A).size());
        assertEquals(2, drained.get(DIR_B).size());
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.drainAll().isEmpty());
    }

    @Test
    public void constructorRejectsNonPositiveThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new SampleBuffer(0));
        assertThrows(IllegalArgumentException.class, () -> new SampleBuffer(-1));
    }
}