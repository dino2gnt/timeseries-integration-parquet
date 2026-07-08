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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;

import com.google.re2j.Pattern;

/**
 * In-memory index of the full {@link Metric} definitions known to the storage, keyed by the
 * metric's composite intrinsic key ({@link Metric#getKey()}).
 *
 * <p>The catalog is the authority for {@code findMetrics}: it holds the complete metric
 * (intrinsic + meta + external tags) so a search can round-trip every tag category exactly,
 * as the storage contract requires. Searches are evaluated over the <em>searchable</em> tags
 * only &mdash; intrinsic and meta tags &mdash; because external tags are collected data that
 * is explicitly not searchable.</p>
 *
 * <p>Storing by {@code getKey()} makes {@link #put(Metric)} an upsert keyed on metric identity
 * (which is defined by the intrinsic tags): re-storing a metric with the same intrinsic tags
 * refreshes its meta/external tags without creating a duplicate entry.</p>
 *
 * <p>All operations are thread-safe.</p>
 */
public class MetricCatalog {

    private final Map<String, Metric> metricsByKey = new ConcurrentHashMap<>();

    /** Set whenever the set of metrics (or a metric's meta/external tags) changes since the last {@link #markClean()}. */
    private volatile boolean dirty = false;

    /** Inserts or refreshes a metric, keyed on its intrinsic identity. */
    public void put(final Metric metric) {
        Objects.requireNonNull(metric, "metric");
        final Metric previous = metricsByKey.put(metric.getKey(), metric);
        // Only a genuinely new metric or changed meta/external tags need re-persisting; a plain
        // re-store of an identical metric (the common case) leaves the catalog clean.
        if (previous == null
                || !previous.getMetaTags().equals(metric.getMetaTags())
                || !previous.getExternalTags().equals(metric.getExternalTags())) {
            dirty = true;
        }
    }

    /** Inserts or refreshes every metric in the collection. */
    public void putAll(final Collection<Metric> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        metrics.forEach(this::put);
    }

    /** Removes a metric from the catalog (logical delete). No-op if it is not present. */
    public void remove(final Metric metric) {
        Objects.requireNonNull(metric, "metric");
        if (metricsByKey.remove(metric.getKey()) != null) {
            dirty = true;
        }
    }

    /** True if the catalog has changed since the last {@link #markClean()} and should be re-persisted. */
    public boolean isDirty() {
        return dirty;
    }

    /** Records that the catalog's current contents have been persisted. */
    public void markClean() {
        dirty = false;
    }

    /** Forces the catalog to be considered unpersisted (e.g. after a failed flush). */
    public void markDirty() {
        dirty = true;
    }

    /** Returns the catalogued metric with the given composite key, or {@code null} if absent. */
    public Metric get(final String key) {
        Objects.requireNonNull(key, "key");
        return metricsByKey.get(key);
    }

    /** A snapshot of all catalogued metrics, safe to iterate while the catalog is mutated. */
    public Collection<Metric> metrics() {
        return new ArrayList<>(metricsByKey.values());
    }

    /** Number of distinct metrics in the catalog. */
    public int size() {
        return metricsByKey.size();
    }

    public boolean isEmpty() {
        return metricsByKey.isEmpty();
    }

    /**
     * Returns every catalogued metric whose searchable tags satisfy <em>all</em> of the given
     * matchers. A matcher is satisfied when at least one searchable tag (intrinsic or meta)
     * shares its key and matches its value per the matcher's {@link TagMatcher.Type}.
     *
     * @throws NullPointerException     if {@code matchers} is null (per the storage contract)
     * @throws IllegalArgumentException if {@code matchers} is empty (per the storage contract)
     */
    public List<Metric> findMetrics(final Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers");
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("We expect at least one TagMatcher but none was given.");
        }
        return metricsByKey.values().stream()
                .filter(metric -> matchesAll(matchers, metric))
                .collect(Collectors.toList());
    }

    /** Every matcher must be satisfied by at least one of the metric's searchable tags. */
    private static boolean matchesAll(final Collection<TagMatcher> matchers, final Metric metric) {
        final Set<Tag> searchableTags = new HashSet<>(metric.getIntrinsicTags());
        searchableTags.addAll(metric.getMetaTags());
        // External tags are intentionally excluded: they are not searchable.

        for (final TagMatcher matcher : matchers) {
            if (searchableTags.stream().noneMatch(tag -> matches(matcher, tag))) {
                return false;
            }
        }
        return true;
    }

    /** True if the tag shares the matcher's key and its value satisfies the matcher's type. */
    private static boolean matches(final TagMatcher matcher, final Tag tag) {
        if (!Objects.equals(matcher.getKey(), tag.getKey())) {
            return false;
        }
        // Tag values are never null.
        switch (matcher.getType()) {
            case EQUALS:
                return tag.getValue().equals(matcher.getValue());
            case NOT_EQUALS:
                return !tag.getValue().equals(matcher.getValue());
            case EQUALS_REGEX:
                return Pattern.matches(matcher.getValue(), tag.getValue());
            case NOT_EQUALS_REGEX:
                return !Pattern.matches(matcher.getValue(), tag.getValue());
            default:
                throw new IllegalArgumentException("Unsupported TagMatcher type: " + matcher.getType());
        }
    }
}