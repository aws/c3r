// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.UnknownNullness;
import lombok.NonNull;

import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Utility functions to convert values from one type to another based off of column specifications.
 */
public final class ValueConverter {
    /**
     * Private utility class constructor.
     */
    private ValueConverter() {
    }

    /**
     * Gets the ClientDataInfo from the start of an encoded value.
     *
     * @param buffer   Encoded C3R data type
     * @param expected Expected ClientDataType
     * @return Basic information on encoded value
     * @throws C3rRuntimeException if the encoded value doesn't have enough bytes or an unexpected type
     */
    private static ClientDataInfo stripClientDataInfo(@NonNull final ByteBuffer buffer, @NonNull final ClientDataType expected) {
        if (buffer.remaining() < ClientDataInfo.BYTE_LENGTH) {
            throw new C3rRuntimeException("Value could not be decoded, not enough bytes.");
        }
        final ClientDataInfo info = ClientDataInfo.decode(buffer.get());
        if (info.getType() != expected) {
            throw new C3rRuntimeException("Expected to decode " + expected + " but found " + info.getType() + " instead.");
        }
        return info;
    }

    /**
     * Find the equivalence class super type for the value and return the value represented in that byte format.
     *
     * @param value Value to convert to equivalence class super type
     * @return byte representation of value in super class
     */
    private static byte[] getBytesForFingerprint(@NonNull final Value value) {
        final ClientDataType superType = value.getClientDataType().getRepresentativeType();
        return value.getBytesAs(superType);
    }

    /**
     * Gets the byte representation of the value according to the column type and any specified conversions.
     *
     * @param value      Value to get bytes from
     * @param columnType Type of column being written
     * @return byte representation of value in the form of the desired {@code ClientDataType}
     */
    public static byte[] getBytesForColumn(@NonNull final Value value, @NonNull final ColumnType columnType) {
        if (columnType == ColumnType.FINGERPRINT) {
            return getBytesForFingerprint(value);
        } else {
            return value.getBytes();
        }
    }

    /**
     * Gets the target data type of the column.
     *
     * @param value      Input value
     * @param columnType Type of column being written
     * @return {@code ClientDataType} contained in column output
     */
    public static ClientDataType getClientDataTypeForColumn(@NonNull final Value value, @NonNull final ColumnType columnType) {
        if (columnType == ColumnType.FINGERPRINT) {
            return value.getClientDataType().getRepresentativeType();
        } else {
            return value.getClientDataType();
        }
    }

    /**
     * Utility functions for converting a C3R BigInt to and from various representations.
     */
    public static final class BigInt {
        /**
         * Convert a big-endian formatted byte array to its long value.
         * Byte array must be {@value Long#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding long value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Long fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length > ClientDataType.BIGINT_BYTE_SIZE) {
                throw new C3rRuntimeException("BigInt values must be " + ClientDataType.BIGINT_BYTE_SIZE + " bytes or less.");
            }
            try {
                return new BigInteger(bytes).longValueExact();
            } catch (ArithmeticException e) {
                throw new C3rRuntimeException("Value out of range of a BigInt.");
            }
        }

        /**
         * Convert an int value to a long big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Integer value) {
            if (value == null) {
                return null;
            }
            return toBytes(value.longValue());
        }

        /**
         * Convert a long value to its big-endian byte representation.
         *
         * @param value Long
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Long value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(value).array();
        }

        /**
         * Encodes a BigInt value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value BigInt value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Long value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.BIGINT).isNull(value == null).build();
            final int length = (value == null) ? 0 : ClientDataType.BIGINT_BYTE_SIZE;
            final ByteBuffer buffer = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putLong(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original BigInt value.
         *
         * @param bytes Encoded value and metadata
         * @return BigInt value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not BigInt
         */
        public static Long decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.BIGINT);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getLong();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Boolean to and from various representations.
     */
    public static final class Boolean {
        /**
         * Convert value to boolean.
         *
         * @param bytes byte encoded value
         * @return {@code true} if value is non-zero or {@code false}
         */
        @UnknownNullness
        public static java.lang.Boolean fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            boolean nonZero = false;
            for (var b : bytes) {
                nonZero |= (b != 0);
            }
            return nonZero;
        }

        /**
         * Take a boolean value and convert it to a 1 byte long byte array.
         *
         * @param value {@code true}, {@code false} or {@code null}
         * @return {@code 1}, {@code 0} or {@code null}
         */
        public static byte[] toBytes(final java.lang.Boolean value) {
            if (value == null) {
                return null;
            } else if (value) {
                return new byte[]{(byte) 1};
            } else {
                return new byte[]{(byte) 0};
            }
        }

        /**
         * Encodes a Boolean value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Boolean value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Boolean value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.BOOLEAN).isNull(value == null).build();
            final int length = (value == null) ? 0 : 1;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.put(toBytes(value));
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original Boolean value.
         *
         * @param bytes Encoded value and metadata
         * @return Boolean value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Boolean
         */
        @UnknownNullness
        public static java.lang.Boolean decode(final byte[] bytes) {

            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.BOOLEAN);
            if (info.isNull()) {
                return null;
            }
            try {
                return fromBytes(new byte[]{buffer.get()});
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Date to and from various representations.
     */
    public static final class Date {
        /**
         * Get the number of day ticks since epoch.
         *
         * @param bytes Byte representation of the ticks
         * @return Number of days since epoch
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        public static Integer fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            if (bytes.length != ClientDataType.INT_BYTE_SIZE) {
                throw new C3rRuntimeException("DATE should be " + ClientDataType.INT_BYTE_SIZE + " in length but " + bytes.length +
                        " found.");
            }
            return ByteBuffer.wrap(bytes).getInt();
        }

        /**
         * Converts the number of ticks since epoch to its byte representation.
         *
         * @param value Number of ticks since epoch
         * @return Byte representation of number
         */
        public static byte[] toBytes(final Integer value) {
            return Int.toBytes(value);
        }

        /**
         * Encodes a Date value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Date value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Integer value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.DATE).isNull(value == null).build();
            final int length = (value == null) ? 0 : Integer.BYTES;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putInt(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original Date value.
         *
         * @param bytes Encoded value and metadata
         * @return Date value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Date
         */
        public static Integer decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.DATE);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getInt();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Double to and from various representations.
     */
    public static final class Double {
        /**
         * Converts a big-endian formatted byte array to its double value.
         * Number of bytes must be {@value java.lang.Double#BYTES}.
         *
         * @param bytes Bytes in big-endian format
         * @return Corresponding float value
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        static java.lang.Double fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length != java.lang.Double.BYTES) {
                throw new C3rRuntimeException("Double values may only be " + java.lang.Double.BYTES + " bytes long.");
            }
            return ByteBuffer.wrap(bytes).getDouble();
        }

        /**
         * Convert a double value to its big-endian byte representation.
         *
         * @param value Double
         * @return Big-endian encoding of value
         */
        public static byte[] toBytes(final java.lang.Double value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(value).array();
        }

        /**
         * Encodes a Double value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Double value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Double value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.DOUBLE).isNull(value == null).build();
            final int length = (value == null) ? 0 : java.lang.Double.BYTES;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putDouble(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original Double value.
         *
         * @param bytes Encoded value and metadata
         * @return Double value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Double
         */
        public static java.lang.Double decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.DOUBLE);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getDouble();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Float to and from various representations.
     */
    public static final class Float {
        /**
         * Converts big-endian formatted bytes to float value.
         * Number of bytes must be {@value java.lang.Float#BYTES}.
         *
         * @param bytes Bytes in big-endian format
         * @return Corresponding float value
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        public static java.lang.Float fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length != java.lang.Float.BYTES) {
                throw new C3rRuntimeException("Float values may only be " + java.lang.Float.BYTES + " bytes long.");
            }
            return ByteBuffer.wrap(bytes).getFloat();
        }

        /**
         * Convert a float value to its big-endian byte representation.
         *
         * @param value Float
         * @return Big-endian encoding of value
         */
        public static byte[] toBytes(final java.lang.Float value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(java.lang.Float.BYTES).putFloat(value).array();
        }

        /**
         * Encodes a Float value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Float value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Float value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.FLOAT).isNull(value == null).build();
            final int length = (value == null) ? 0 : java.lang.Float.BYTES;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putFloat(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original Float value.
         *
         * @param bytes Encoded value and metadata
         * @return Float value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Float
         */
        public static java.lang.Float decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.FLOAT);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getFloat();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Int to and from various representations.
     */
    public static final class Int {
        /**
         * Convert a big-endian formatted byte array to its integer value.
         * Byte array must be {@value Integer#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding integer value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Integer fromBytes(@Nullable final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length > ClientDataType.INT_BYTE_SIZE) {
                throw new C3rRuntimeException("Integer values must be " + ClientDataType.INT_BYTE_SIZE + " bytes or less.");
            }
            try {
                return new BigInteger(bytes).intValueExact();
            } catch (ArithmeticException e) {
                throw new C3rRuntimeException("Value out of range of an Int.");
            }
        }

        /**
         * Convert an integer value to its big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Integer value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(ClientDataType.INT_BYTE_SIZE).putInt(value).array();
        }

        /**
         * Encodes an Int value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Int value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Integer value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.INT).isNull(value == null).build();
            final int length = (value == null) ? 0 : ClientDataType.INT_BYTE_SIZE;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putInt(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original Int value.
         *
         * @param bytes Encoded value and metadata
         * @return Int value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Int
         */
        public static Integer decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.INT);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getInt();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R SmallInt to and from various representations.
     */
    public static final class SmallInt {
        /**
         * Convert a big-endian formatted byte array to its integer value.
         * Byte array must be {@value Integer#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding integer value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Short fromBytes(@Nullable final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length > ClientDataType.SMALLINT_BYTE_SIZE) {
                throw new C3rRuntimeException("Integer values must be " + ClientDataType.INT_BYTE_SIZE + " bytes or less.");
            }
            try {
                return new BigInteger(bytes).shortValueExact();
            } catch (ArithmeticException e) {
                throw new C3rRuntimeException("Value out of range of SmallInt.", e);
            }
        }

        /**
         * Convert an integer value to its big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Short value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(ClientDataType.SMALLINT_BYTE_SIZE).putShort(value).array();
        }

        /**
         * Encodes a SmallInt value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value SmallInt value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Short value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.SMALLINT).isNull(value == null).build();
            final int length = (value == null) ? 0 : ClientDataType.SMALLINT_BYTE_SIZE;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                buffer.putShort(value);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original SmallInt value.
         *
         * @param bytes Encoded value and metadata
         * @return SmallInt value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not SmallInt
         */
        public static Short decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.SMALLINT);
            if (info.isNull()) {
                return null;
            }
            try {
                return buffer.getShort();
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R String to and from various representations.
     */
    public static final class String {
        /**
         * Convert the byte array to a UTF-8 String.
         *
         * @param bytes Bytes representing string value
         * @return UTF-8 string generated from bytes
         */
        public static java.lang.String fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).toString();
        }

        /**
         * Convert a string to the UTF-8 bytes that represent its value.
         *
         * @param value String to conver to bytes
         * @return UTF-8 byte representation
         */
        public static byte[] toBytes(final java.lang.String value) {
            if (value == null) {
                return null;
            }
            return value.getBytes(StandardCharsets.UTF_8);
        }

        /**
         * Encodes a String value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value String value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.String value) {
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.STRING).isNull(value == null).build();
            final byte[] bytes = toBytes(value);
            final int length = (bytes == null) ? 0 : bytes.length;
            final ByteBuffer buffer =  ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (bytes != null) {
                buffer.put(bytes);
            }
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original String value.
         *
         * @param bytes Encoded value and metadata
         * @return String value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not String
         */
        public static java.lang.String decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.STRING);
            if (info.isNull()) {
                return null;
            }
            try {
                final byte[] strBuffer = new byte[bytes.length - ClientDataInfo.BYTE_LENGTH];
                buffer.get(strBuffer, 0, strBuffer.length);
                return fromBytes(strBuffer);
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }
}
