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

import java.util.Collection;

import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;

/**
 * Operator-facing maintenance hooks for the parquet store, exposed as an OSGi service so the Karaf
 * shell commands (e.g. {@code tss-parquet:compact}, {@code tss-parquet:import-from-rrd}) can drive
 * them on demand rather than waiting for the scheduled background timers.
 */
public interface ParquetMaintenance {

    /**
     * Runs a single compaction pass synchronously and returns when it finishes. The pass runs on the
     * store's own single maintenance thread, so it never races the scheduled sweep/compaction.
     *
     * @return the number of partitions that were modified (small files merged and/or empty files removed)
     * @throws StorageException if the store is not initialized or the pass failed
     */
    int compactNow() throws StorageException;

    /**
     * Bulk-imports samples, grouping by {@code (shard,partition)} and writing each partition's rows
     * as one file, bypassing the async write buffer. Ideal for a one-shot historical backfill (e.g.
     * from RRD/JRB): new partitions get a single file needing no compaction, while partitions that
     * already hold data gain one file a later compaction pass consolidates. Metrics are indexed in
     * memory; call {@link #flushCatalog()} once when the whole import is done to persist them.
     *
     * @return the number of partition files written
     */
    int bulkImport(Collection<Sample> samples) throws StorageException;

    /** Persists any pending catalog (metric index) changes to the durable sidecar. Call once after a bulk import. */
    void flushCatalog() throws StorageException;
}