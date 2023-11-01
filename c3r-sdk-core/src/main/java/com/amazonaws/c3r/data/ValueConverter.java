// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.UnknownNullness;
import lombok.NonNull;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;

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
     * Utility functions for converting a C3R BigInt to and from byte representation.
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
            return new BigInteger(bytes).longValue();
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
        static byte[] toBytes(final Long value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(value).array();
        }
    }

    /**
     * Utility functions for converting a C3R boolean to and from byte representation.
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
    }

    /**
     * Utility functions for converting a C3R Date to and from byte representation.
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
            if (bytes.length != INT_BYTE_SIZE) {
                throw new C3rRuntimeException("DATE should be " + INT_BYTE_SIZE + " in length but " + bytes.length + " found.");
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
    }

    /**
     * Utility functions for converting a C3R Double to and from byte representation.
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
        static byte[] toBytes(final java.lang.Double value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(value).array();
        }
    }

    /**
     * Utility functions for converting a C3R float to and from byte representation.
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
    }

    /**
     * Utility functions for converting a C3R Int to and from byte representation.
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
            } else if (bytes.length > INT_BYTE_SIZE) {
                throw new C3rRuntimeException("Integer values must be " + INT_BYTE_SIZE + " bytes or less.");
            }
            return new BigInteger(bytes).intValue();
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
            return ByteBuffer.allocate(INT_BYTE_SIZE).putInt(value).array();
        }
    }

    /**
     * Utility functions for converting a C3R String to and from byte representation.
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
    }
}
