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

import java.time.Instant;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * The Parquet schema for a sample file and the timestamp conversions shared by the reader and
 * writer.
 *
 * <p>A sample row is {@code {time, name, value}}. The {@code resourceId} is implicit in the
 * shard directory path (see {@link PathMapper}) and therefore not stored per row. {@code time}
 * is epoch microseconds so it preserves the {@link Instant} precision the storage contract uses;
 * {@code name} is the datasource name (the {@code name} intrinsic tag); {@code value} is the
 * sample value.</p>
 */
final class SampleSchema {

    static final String FIELD_TIME = "time";
    static final String FIELD_NAME = "name";
    static final String FIELD_VALUE = "value";

    /** Column indices, matching the field order in {@link #SCHEMA}. */
    static final int IDX_TIME = 0;
    static final int IDX_NAME = 1;
    static final int IDX_VALUE = 2;

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    static final MessageType SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.INT64).named(FIELD_TIME)
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(FIELD_NAME)
            .required(PrimitiveTypeName.DOUBLE).named(FIELD_VALUE)
            .named("sample");

    private SampleSchema() {
    }

    /** Instant &rarr; epoch microseconds. */
    static long toEpochMicros(final Instant instant) {
        return Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND)
                + instant.getNano() / NANOS_PER_MICRO;
    }

    /** Epoch microseconds &rarr; Instant (inverse of {@link #toEpochMicros(Instant)}). */
    static Instant fromEpochMicros(final long micros) {
        final long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
        final long microOfSecond = Math.floorMod(micros, MICROS_PER_SECOND);
        return Instant.ofEpochSecond(seconds, microOfSecond * NANOS_PER_MICRO);
    }
}