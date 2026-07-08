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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory accumulation of not-yet-written samples, grouped by their target partition directory
 * (shard + time partition).
 *
 * <p>Writing a parquet file per {@code store} call would produce a swarm of tiny files. Instead
 * samples are buffered here and drained into a single {@code part-*.parquet} per bucket when the
 * bucket reaches {@code maxRowsPerBucket} rows, on the periodic flush timer, or at shutdown (see
 * {@link ParquetStorage}). Buffered samples are held only in memory, so an unclean crash loses at
 * most what has not yet been flushed — an accepted trade-off for columnar batching (DESIGN §5).</p>
 *
 * <p>All state is guarded by a single lock. Callers add rows under the lock but must perform the
 * actual (slow) parquet write on the drained snapshot <em>outside</em> it.</p>
 */
class SampleBuffer {

    private final int maxRowsPerBucket;
    private final Object lock = new Object();
    private final Map<Path, List<SampleRow>> bucketsByPartition = new HashMap<>();

    SampleBuffer(final int maxRowsPerBucket) {
        if (maxRowsPerBucket <= 0) {
            throw new IllegalArgumentException("maxRowsPerBucket must be positive: " + maxRowsPerBucket);
        }
        this.maxRowsPerBucket = maxRowsPerBucket;
    }

    /**
     * Buffers a row for the given partition directory.
     *
     * @return {@code true} if the bucket has reached the flush threshold and should be drained
     */
    boolean add(final Path partitionDir, final SampleRow row) {
        synchronized (lock) {
            final List<SampleRow> bucket = bucketsByPartition.computeIfAbsent(partitionDir, k -> new ArrayList<>());
            bucket.add(row);
            return bucket.size() >= maxRowsPerBucket;
        }
    }

    /**
     * Atomically removes and returns the buffered rows for one partition, or {@code null} if the
     * bucket is empty/absent. Rows added after this call accumulate into a fresh bucket.
     */
    List<SampleRow> drainBucket(final Path partitionDir) {
        synchronized (lock) {
            return bucketsByPartition.remove(partitionDir);
        }
    }

    /** Atomically removes and returns every buffered bucket, leaving the buffer empty. */
    Map<Path, List<SampleRow>> drainAll() {
        synchronized (lock) {
            if (bucketsByPartition.isEmpty()) {
                return Map.of();
            }
            final Map<Path, List<SampleRow>> drained = new HashMap<>(bucketsByPartition);
            bucketsByPartition.clear();
            return drained;
        }
    }

    boolean isEmpty() {
        synchronized (lock) {
            return bucketsByPartition.isEmpty();
        }
    }
}