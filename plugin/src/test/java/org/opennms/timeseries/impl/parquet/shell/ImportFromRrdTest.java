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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.Test;
import org.opennms.timeseries.impl.parquet.util.rrd.ResourcePath;

/** resourceId path mapping for the RRD import, focused on honoring org.opennms.rrd.storeByForeignSource. */
public class ImportFromRrdTest {

    private static final Map<Integer, ImportFromRrd.ForeignId> NODES =
            Map.of(5, new ImportFromRrd.ForeignId("MyReq", "node5"));

    @Test
    public void storeByIdKeepsNumericPathWhenStoreByForeignSourceIsFalse() {
        // The bug fix: with storeByForeignSource=false the on-disk snmp/<nodeId>/... path must be
        // used verbatim, NOT rewritten to snmp/fs/..., so imported resourceIds match live collection.
        final Path rel = Paths.get("snmp", "5", "eth0-04", "mib2-interfaces");
        assertEquals("snmp/5/eth0-04/mib2-interfaces",
                ImportFromRrd.buildResourcePath(rel, false, NODES).toString());
    }

    @Test
    public void storeByIdIsConvertedToForeignSourceWhenTrue() {
        final Path rel = Paths.get("snmp", "5", "eth0-04", "mib2-interfaces");
        assertEquals("snmp/fs/MyReq/node5/eth0-04/mib2-interfaces",
                ImportFromRrd.buildResourcePath(rel, true, NODES).toString());
    }

    @Test
    public void alreadyForeignSourcePathIsKeptAsIsInForeignSourceMode() {
        final Path rel = Paths.get("snmp", "fs", "MyReq", "node5", "mib2-interfaces");
        assertEquals("snmp/fs/MyReq/node5/mib2-interfaces",
                ImportFromRrd.buildResourcePath(rel, true, NODES).toString());
    }

    @Test
    public void unknownNodeInForeignSourceModeYieldsNull() {
        final Path rel = Paths.get("snmp", "999", "mib2-interfaces");
        assertNull(ImportFromRrd.buildResourcePath(rel, true, NODES));
    }

    @Test
    public void responsePathIsKeptAsIsRegardlessOfMode() {
        final Path rel = Paths.get("response", "192.168.1.1", "icmp");
        assertEquals("response/192.168.1.1/icmp",
                ImportFromRrd.buildResourcePath(rel, false, NODES).toString());
        assertEquals("response/192.168.1.1/icmp",
                ImportFromRrd.buildResourcePath(rel, true, NODES).toString());
    }
}