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
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

/**
 * A pure-JVM {@link CompressionCodecFactory} supporting {@code UNCOMPRESSED} and {@code LZ4_RAW}.
 *
 * <p>Parquet's own {@code CodecFactory} instantiates codecs through a Hadoop {@code Configuration},
 * whose class initialization drags in woodstox and the rest of Hadoop's transitive tree that this
 * plugin deliberately excludes (DESIGN §9/§10). Supplying this factory to the writer/reader
 * (via {@code withCodecFactory}) bypasses that entirely: {@code LZ4_RAW} is handled directly by
 * aircompressor's pure-Java LZ4 (byte-compatible with parquet's {@code Lz4RawCodec}, which wraps
 * the same library), and {@code UNCOMPRESSED} is a pass-through.</p>
 *
 * <p>Per the interface contract compressor/decompressor instances are not thread-safe; a caller
 * uses one factory per {@code ParquetWriter}/{@code ParquetReader}, which is single-threaded.</p>
 */
class AircompressorCodecFactory implements CompressionCodecFactory {

    @Override
    public BytesInputCompressor getCompressor(final CompressionCodecName codecName) {
        switch (codecName) {
            case UNCOMPRESSED:
                return new PassthroughCompressor();
            case LZ4_RAW:
                return new Lz4RawCompressor();
            default:
                throw new IllegalArgumentException("Unsupported compression codec: " + codecName
                        + " (supported: UNCOMPRESSED, LZ4_RAW)");
        }
    }

    @Override
    public BytesInputDecompressor getDecompressor(final CompressionCodecName codecName) {
        switch (codecName) {
            case UNCOMPRESSED:
                return new PassthroughDecompressor();
            case LZ4_RAW:
                return new Lz4RawDecompressor();
            default:
                throw new IllegalArgumentException("Unsupported compression codec: " + codecName
                        + " (supported: UNCOMPRESSED, LZ4_RAW)");
        }
    }

    @Override
    public void release() {
        // Nothing to release; compressors/decompressors hold no pooled resources.
    }

    private static final class PassthroughCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(final BytesInput bytes) {
            return bytes;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public void release() {
        }
    }

    private static final class PassthroughDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(final BytesInput bytes, final int decompressedSize) {
            return bytes;
        }

        @Override
        public void decompress(final ByteBuffer input, final int compressedSize,
                               final ByteBuffer output, final int decompressedSize) {
            final int originalLimit = input.limit();
            input.limit(input.position() + compressedSize);
            output.put(input);
            input.limit(originalLimit);
        }

        @Override
        public void release() {
        }
    }

    private static final class Lz4RawCompressor implements BytesInputCompressor {
        private final Lz4Compressor compressor = new Lz4Compressor();

        @Override
        public BytesInput compress(final BytesInput bytes) throws IOException {
            final byte[] input = bytes.toByteArray();
            final byte[] output = new byte[compressor.maxCompressedLength(input.length)];
            final int compressedLength = compressor.compress(input, 0, input.length, output, 0, output.length);
            return BytesInput.from(output, 0, compressedLength);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.LZ4_RAW;
        }

        @Override
        public void release() {
        }
    }

    private static final class Lz4RawDecompressor implements BytesInputDecompressor {
        private final Lz4Decompressor decompressor = new Lz4Decompressor();

        @Override
        public BytesInput decompress(final BytesInput bytes, final int decompressedSize) throws IOException {
            final byte[] input = bytes.toByteArray();
            final byte[] output = new byte[decompressedSize];
            decompressor.decompress(input, 0, input.length, output, 0, decompressedSize);
            return BytesInput.from(output, 0, decompressedSize);
        }

        @Override
        public void decompress(final ByteBuffer input, final int compressedSize,
                               final ByteBuffer output, final int decompressedSize) {
            final int originalLimit = input.limit();
            input.limit(input.position() + compressedSize);
            decompressor.decompress(input, output);
            input.limit(originalLimit);
        }

        @Override
        public void release() {
        }
    }
}
