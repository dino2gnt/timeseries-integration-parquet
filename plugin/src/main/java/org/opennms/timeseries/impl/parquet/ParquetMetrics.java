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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;

/**
 * Dropwizard (Codahale) metrics for the parquet store, exported over JMX under the domain
 * {@value #JMX_DOMAIN}.
 *
 * <p>The registry and instruments are created eagerly so callers can record into them at any time
 * (even before {@link #start()}); {@link #start()} attaches the {@link JmxReporter} that publishes
 * them as MBeans, and {@link #stop()} detaches it (unregistering the MBeans) on shutdown.</p>
 *
 * <ul>
 *   <li>{@code compaction} &mdash; a {@link Timer} (duration histogram) over each compaction pass</li>
 *   <li>{@code samples.written} / {@code samples.read} &mdash; total samples stored / read from disk</li>
 *   <li>{@code partitions.created} / {@code partitions.deleted} / {@code partitions.compacted}
 *       &mdash; partition directories created by writes, removed by retention, modified by compaction</li>
 * </ul>
 */
class ParquetMetrics {

    static final String JMX_DOMAIN = "org.opennms.timeseries.impl.tss-parquet";

    private final MetricRegistry registry = new MetricRegistry();
    private final Timer compaction = registry.timer("compaction");
    private final Counter samplesWritten = registry.counter("samples.written");
    private final Counter samplesRead = registry.counter("samples.read");
    private final Counter partitionsCreated = registry.counter("partitions.created");
    private final Counter partitionsDeleted = registry.counter("partitions.deleted");
    private final Counter partitionsCompacted = registry.counter("partitions.compacted");

    private JmxReporter reporter;

    /** Attaches the JMX reporter so the metrics are visible as MBeans. Idempotent. */
    synchronized void start() {
        if (reporter == null) {
            reporter = JmxReporter.forRegistry(registry).inDomain(JMX_DOMAIN).build();
            reporter.start();
        }
    }

    /** Detaches the JMX reporter, unregistering the MBeans. Idempotent. */
    synchronized void stop() {
        if (reporter != null) {
            reporter.close();
            reporter = null;
        }
    }

    /** The underlying registry, for reporters (e.g. the {@code tss-parquet:stats} ConsoleReporter). */
    MetricRegistry registry() {
        return registry;
    }

    /** The compaction-duration timer; time each pass with {@code try (var t = compactionTimer().time())}. */
    Timer compactionTimer() {
        return compaction;
    }

    void incSamplesWritten(final long n) {
        samplesWritten.inc(n);
    }

    void incSamplesRead(final long n) {
        samplesRead.inc(n);
    }

    void incPartitionsCreated() {
        partitionsCreated.inc();
    }

    void incPartitionsDeleted(final long n) {
        partitionsDeleted.inc(n);
    }

    void incPartitionsCompacted(final long n) {
        partitionsCompacted.inc(n);
    }
}