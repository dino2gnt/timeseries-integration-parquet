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

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.opennms.integration.api.v1.timeseries.AbstractStorageIntegrationTest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;

/**
 * Contract test for the storage implementation.
 *
 * <p>{@link AbstractStorageIntegrationTest} (from {@code org.opennms.integration.api:tss-tests})
 * exercises the full {@link TimeSeriesStorage} contract - store, findMetrics, getTimeseries,
 * delete and aggregation - against whatever {@link #createStorage()} returns.</p>
 *
 * <p>Each test gets a fresh {@link ParquetStorage} rooted at a throwaway temp directory and driven
 * through the real OSGi lifecycle ({@code init()}/{@code destroy()}). Sample writes are buffered
 * asynchronously, so {@code waitForPersistingChanges()} forces a flush before the contract reads
 * back &mdash; exactly what that hook is for.</p>
 */
public class ParquetStorageTest extends AbstractStorageIntegrationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ParquetStorage parquetStorage;

    @Override
    protected TimeSeriesStorage createStorage() throws Exception {
        parquetStorage = new ParquetStorage(tempFolder.newFolder("parquet-data").toPath());
        parquetStorage.init();
        return parquetStorage;
    }

    @Override
    protected void waitForPersistingChanges() throws Exception {
        parquetStorage.flush();
    }

    @After
    public void tearDown() {
        if (parquetStorage != null) {
            parquetStorage.destroy();
        }
    }
}