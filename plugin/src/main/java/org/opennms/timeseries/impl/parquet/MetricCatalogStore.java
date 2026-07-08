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

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

/**
 * Reads and writes the durable metric-catalog sidecar ({@code <baseDir>/catalog/metrics.parquet}).
 *
 * <p>The sidecar is the <em>only</em> place a metric's meta and external tags are persisted: the
 * sample files store just {@code name/time/value}, so on restart the full metric definitions can
 * only be recovered from here. It is a flat parquet file with one row per tag &mdash;
 * {@code (metricKey, tagType, tagKey, tagValue)} &mdash; where {@code tagType} is
 * {@code intrinsic}/{@code meta}/{@code external} and {@code tagKey} is optional (a {@link Tag}
 * key may be null). Reading groups the rows by {@code metricKey} and rebuilds each
 * {@link ImmutableMetric} with all three tag categories.</p>
 *
 * <p>Writes are whole-file and atomic: a temp file is written and then moved into place, so a
 * crash mid-write never leaves a truncated catalog. The catalog changes rarely (only when a new
 * metric appears or one is deleted), so rewriting it wholesale is cheap in practice.</p>
 */
class MetricCatalogStore {

    static final String TYPE_INTRINSIC = "intrinsic";
    static final String TYPE_META = "meta";
    static final String TYPE_EXTERNAL = "external";

    private static final int IDX_METRIC_KEY = 0;
    private static final int IDX_TAG_TYPE = 1;
    private static final int IDX_TAG_KEY = 2;
    private static final int IDX_TAG_VALUE = 3;

    private static final MessageType SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("metricKey")
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("tagType")
            .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("tagKey")
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("tagValue")
            .named("metric_catalog");

    private static final String TEMP_SUFFIX = ".tmp";

    /** Atomically (re)writes the sidecar with the given metrics, creating the catalog dir if needed. */
    void write(final Path file, final Iterable<Metric> metrics) throws IOException {
        Files.createDirectories(file.getParent());
        final Path temp = file.resolveSibling(file.getFileName() + TEMP_SUFFIX);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(temp))
                .withType(SCHEMA)
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (final Metric metric : metrics) {
                writeTags(writer, metric.getKey(), TYPE_INTRINSIC, metric.getIntrinsicTags());
                writeTags(writer, metric.getKey(), TYPE_META, metric.getMetaTags());
                writeTags(writer, metric.getKey(), TYPE_EXTERNAL, metric.getExternalTags());
            }
        }
        moveIntoPlace(temp, file);
    }

    private static void writeTags(final ParquetWriter<Group> writer, final String metricKey,
                                  final String tagType, final Iterable<Tag> tags) throws IOException {
        for (final Tag tag : tags) {
            final Group group = new SimpleGroup(SCHEMA);
            group.add(IDX_METRIC_KEY, metricKey);
            group.add(IDX_TAG_TYPE, tagType);
            if (tag.getKey() != null) {
                group.add(IDX_TAG_KEY, tag.getKey());
            }
            group.add(IDX_TAG_VALUE, tag.getValue());
            writer.write(group);
        }
    }

    private static void moveIntoPlace(final Path temp, final Path file) throws IOException {
        try {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException e) {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Loads all metrics from the sidecar, or an empty list if it does not exist yet. */
    List<Metric> read(final Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            return new ArrayList<>();
        }
        // Preserve first-seen order for stable, debuggable output.
        final Map<String, ImmutableMetric.MetricBuilder> byKey = new LinkedHashMap<>();
        try (ParquetReader<Group> reader = new GroupReaderBuilder(
                new LocalInputFile(file), new PlainParquetConfiguration()).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                final String metricKey = group.getString(IDX_METRIC_KEY, 0);
                final String tagType = group.getString(IDX_TAG_TYPE, 0);
                final String tagKey = group.getFieldRepetitionCount(IDX_TAG_KEY) > 0
                        ? group.getString(IDX_TAG_KEY, 0) : null;
                final String tagValue = group.getString(IDX_TAG_VALUE, 0);
                addTag(byKey.computeIfAbsent(metricKey, k -> ImmutableMetric.builder()), tagType, tagKey, tagValue);
            }
        }
        final List<Metric> metrics = new ArrayList<>(byKey.size());
        for (final ImmutableMetric.MetricBuilder builder : byKey.values()) {
            metrics.add(builder.build());
        }
        return metrics;
    }

    private static void addTag(final ImmutableMetric.MetricBuilder builder, final String tagType,
                               final String tagKey, final String tagValue) {
        switch (tagType) {
            case TYPE_INTRINSIC:
                builder.intrinsicTag(tagKey, tagValue);
                break;
            case TYPE_META:
                builder.metaTag(tagKey, tagValue);
                break;
            case TYPE_EXTERNAL:
                builder.externalTag(tagKey, tagValue);
                break;
            default:
                throw new IllegalStateException("Unknown tag type in catalog sidecar: " + tagType);
        }
    }

    /**
     * A {@link ParquetReader.Builder} that reads the example {@link Group} object model from an
     * {@link InputFile}; the stock {@code read(InputFile)} builder leaves the read support null.
     */
    private static final class GroupReaderBuilder extends ParquetReader.Builder<Group> {
        private GroupReaderBuilder(final InputFile file, final ParquetConfiguration conf) {
            super(file, conf);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }
}