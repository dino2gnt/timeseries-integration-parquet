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

package org.opennms.timeseries.impl.parquet.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.parquet.ParquetMaintenance;

/**
 * {@code tss-parquet:compact} &mdash; runs a parquet compaction pass on demand instead of waiting for
 * the scheduled {@code compaction.interval} timer.
 *
 * <p>This class lives in a dedicated {@code .shell} subpackage on purpose: the bundle's
 * {@code Karaf-Commands} header points at exactly this package, so Karaf's command scanner inspects
 * only the command classes here and never walks the embedded hadoop/parquet classes (which reference
 * deliberately-excluded deps and would otherwise blow up the scan &mdash; see DESIGN.md &sect;10).</p>
 */
@Command(scope = "tss-parquet", name = "compact",
        description = "Run a parquet compaction pass now (merge small part files, drop empty/deleted rows).")
@Service
public class CompactCommand implements Action {

    @Reference
    private ParquetMaintenance maintenance;

    @Override
    public Object execute() throws Exception {
        System.out.println("Compacting...");
        final long startNanos = System.nanoTime();
        final int modified = maintenance.compactNow();
        final long millis = (System.nanoTime() - startNanos) / 1_000_000L;
        System.out.println("Compaction complete: " + modified + " partition(s) modified in " + millis + " ms.");
        return null;
    }

    /** Setter kept for tests; the shell injects via the {@link Reference} field. */
    void setMaintenance(final ParquetMaintenance maintenance) {
        this.maintenance = maintenance;
    }
}