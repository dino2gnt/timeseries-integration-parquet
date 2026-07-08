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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

/**
 * Maps a metric's {@code resourceId} to an on-disk shard directory and a timestamp to a
 * time-partition directory name.
 *
 * <p>The {@code resourceId} intrinsic tag is already a slash-delimited resource path
 * (e.g. {@code snmp/fs/Servers/node1/eth0/mib2-X-interfaces} with storeByForeignSource
 * enabled, or {@code snmp/1/eth0/mib2-X-interfaces} otherwise), so we use it directly as
 * a relative directory path under {@code <baseDir>/data/}. Each path element is
 * percent-encoded so that characters that are unsafe in a filesystem path (or that could
 * escape the base directory, such as {@code .} and {@code ..}) never reach the disk. The
 * {@code /} separators between elements are preserved. The canonical {@code resourceId}
 * lives in the {@link MetricCatalog}, so encoded paths never need to be decoded.</p>
 *
 * <p>Time partitions are self-describing directory names of the form {@code p=<value>},
 * where the value is the sample timestamp truncated to the configured partition duration
 * (e.g. {@code p=2026-07-06} for daily partitions, {@code p=2026-07-06-14} for hourly).
 * This lets the retention sweep derive a partition's cutoff from its directory name alone.</p>
 *
 * <p>Instances are immutable and thread-safe.</p>
 */
public class PathMapper {

    /** Directory (under baseDir) that holds the sharded sample data. */
    static final String DATA_DIR = "data";

    /** Directory (under baseDir) that holds the durable metric-catalog sidecar. */
    static final String CATALOG_DIR = "catalog";

    /** File name of the durable metric-catalog sidecar within {@link #CATALOG_DIR}. */
    static final String CATALOG_FILE = "metrics.parquet";

    /** Prefix of a time-partition directory name. */
    static final String PARTITION_PREFIX = "p=";

    private static final DateTimeFormatter DAY_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter MINUTE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm").withZone(ZoneOffset.UTC);

    private static final long SECONDS_PER_HOUR = 3600L;
    private static final long SECONDS_PER_DAY = 86400L;

    private final Path baseDir;
    private final Path dataRoot;

    /**
     * @param baseDir the plugin's base directory (typically the OpenNMS RRD data root);
     *                the shard tree lives under {@code baseDir/data} and the catalog sidecar
     *                under {@code baseDir/catalog}.
     */
    public PathMapper(final Path baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir");
        this.dataRoot = baseDir.resolve(DATA_DIR);
    }

    /** The root of the sharded data tree ({@code baseDir/data}). */
    public Path dataRoot() {
        return dataRoot;
    }

    /** The durable metric-catalog sidecar file ({@code baseDir/catalog/metrics.parquet}). */
    public Path catalogFile() {
        return baseDir.resolve(CATALOG_DIR).resolve(CATALOG_FILE);
    }

    /**
     * Resolves the shard directory for a resourceId: {@code baseDir/data/<sanitized resourceId>}.
     * The resourceId is split on {@code /} and each element is percent-encoded independently,
     * keeping the {@code /} as a directory separator.
     *
     * @throws NullPointerException     if resourceId is null
     * @throws IllegalArgumentException if resourceId is empty
     */
    public Path shardDir(final String resourceId) {
        Objects.requireNonNull(resourceId, "resourceId");
        if (resourceId.isEmpty()) {
            throw new IllegalArgumentException("resourceId must not be empty");
        }
        Path dir = dataRoot;
        for (final String element : resourceId.split("/", -1)) {
            if (element.isEmpty()) {
                // Collapse empty elements from leading/trailing/duplicate slashes.
                continue;
            }
            dir = dir.resolve(sanitize(element));
        }
        return dir;
    }

    /**
     * Resolves the partition directory for a sample timestamp within a shard:
     * {@code <shardDir>/p=<partition value>}.
     */
    public Path partitionDir(final String resourceId, final Instant timestamp, final Duration partitionDuration) {
        return shardDir(resourceId).resolve(partitionName(timestamp, partitionDuration));
    }

    /**
     * The self-describing partition directory name for a timestamp, e.g. {@code p=2026-07-06}.
     * The timestamp is floored to the partition boundary measured from the epoch.
     *
     * @throws IllegalArgumentException if partitionDuration is not positive
     */
    public String partitionName(final Instant timestamp, final Duration partitionDuration) {
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(partitionDuration, "partitionDuration");
        final long durationSeconds = partitionDuration.getSeconds();
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("partitionDuration must be positive: " + partitionDuration);
        }
        final long flooredSeconds = Math.floorDiv(timestamp.getEpochSecond(), durationSeconds) * durationSeconds;
        final Instant boundary = Instant.ofEpochSecond(flooredSeconds);
        return PARTITION_PREFIX + formatterFor(durationSeconds).format(boundary);
    }

    /**
     * Parses a partition directory name (e.g. {@code p=2026-07-06} or {@code p=2026-07-06-14})
     * back to the instant at the start of that partition window, the inverse of
     * {@link #partitionName}. Returns empty for any name that is not a recognizable partition
     * directory, so callers can safely skip unrelated directories.
     *
     * <p>Parsing is independent of the configured partition duration: the trailing fields
     * (hour, minute) simply default to zero when absent, which recovers the boundary for daily,
     * hourly and sub-hourly labels alike.</p>
     */
    public Optional<Instant> parsePartitionStart(final String partitionDirName) {
        if (partitionDirName == null || !partitionDirName.startsWith(PARTITION_PREFIX)) {
            return Optional.empty();
        }
        final String[] fields = partitionDirName.substring(PARTITION_PREFIX.length()).split("-");
        if (fields.length < 3 || fields.length > 5) {
            return Optional.empty();
        }
        try {
            final int year = Integer.parseInt(fields[0]);
            final int month = Integer.parseInt(fields[1]);
            final int day = Integer.parseInt(fields[2]);
            final int hour = fields.length > 3 ? Integer.parseInt(fields[3]) : 0;
            final int minute = fields.length > 4 ? Integer.parseInt(fields[4]) : 0;
            return Optional.of(LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC));
        } catch (final NumberFormatException | DateTimeException e) {
            return Optional.empty();
        }
    }

    /** Choose the coarsest label that still distinguishes adjacent partitions. */
    private static DateTimeFormatter formatterFor(final long durationSeconds) {
        if (durationSeconds % SECONDS_PER_DAY == 0) {
            return DAY_FORMAT;
        }
        if (durationSeconds % SECONDS_PER_HOUR == 0) {
            return HOUR_FORMAT;
        }
        return MINUTE_FORMAT;
    }

    /**
     * Percent-encodes every character of a single path element that is not in the unreserved
     * set {@code [A-Za-z0-9._-]}, and additionally encodes the dots of an element that is
     * entirely {@code .} or {@code ..} so it can never traverse out of the shard tree.
     */
    static String sanitize(final String element) {
        if (".".equals(element) || "..".equals(element)) {
            // Encode the dots so the element becomes a harmless literal directory name.
            return element.replace(".", "%2E");
        }
        final StringBuilder sb = new StringBuilder(element.length());
        for (int i = 0; i < element.length(); i++) {
            final char c = element.charAt(i);
            if (isUnreserved(c)) {
                sb.append(c);
            } else {
                for (final byte b : String.valueOf(c).getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
                    sb.append('%');
                    sb.append(Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, 16)));
                    sb.append(Character.toUpperCase(Character.forDigit(b & 0xF, 16)));
                }
            }
        }
        return sb.toString();
    }

    private static boolean isUnreserved(final char c) {
        return (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '.' || c == '_' || c == '-';
    }
}