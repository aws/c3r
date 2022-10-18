// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.UnknownNullness;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Common interface for user-provided data entries (i.e., the values that populate
 * each column).
 */
public abstract class Value {
    /**
     * Encode a non-null {@code Value} into a byte representation.
     *
     * @param value The value to encode.
     * @return The value encoded as a byte array, or {@code null} if {@code value == null}.
     */
    public static byte[] getBytes(@Nullable final Value value) {
        if (value == null) {
            return null;
        } else {
            return value.getBytes();
        }
    }

    /**
     * Whether the value is equivalent to {@code null}.
     *
     * @return {@code true} if the value represents a {@code null}
     */
    public abstract boolean isNull();

    /**
     * Get the type of then entry.
     *
     * @return Data type
     * @see ClientDataType
     */
    public abstract ClientDataType getClientDataType();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract int hashCode();

    /**
     * Get data type for entry.
     *
     * <p>
     * {@inheritDoc}
     */
    @Override
    @UnknownNullness
    public abstract String toString();

    /**
     * Convert value to boolean.
     *
     * @param bytes byte encoded value
     * @return {@code true} if value is non-zero or {@code false}
     */
    @UnknownNullness
    static Boolean booleanFromBytes(final byte[] bytes) {
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
    static byte[] booleanToBytes(final java.lang.Boolean value) {
        if (value == null) {
            return null;
        } else if (value) {
            return new byte[]{(byte) 1};
        } else {
            return new byte[]{(byte) 0};
        }
    }

    /**
     * Convert a big-endian formatted byte array to its integer value.
     * Byte array must be {@value Integer#BYTES} or less in length.
     *
     * @param bytes Big-endian formatted byte array
     * @return Corresponding integer value
     * @throws C3rRuntimeException If the byte array is more than the max length
     */
    static Integer intFromBytes(@Nullable final byte[] bytes) {
        if (bytes == null) {
            return null;
        } else if (bytes.length > Integer.BYTES) {
            throw new C3rRuntimeException("Integer values must be " + Integer.BYTES + " bytes or less.");
        }
        return new BigInteger(bytes).intValue();
    }

    /**
     * Convert an integer value to its big-endian byte representation.
     *
     * @param value Integer
     * @return Big-endian byte encoding of value
     */
    static byte[] intToBytes(final Integer value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    /**
     * Convert a big-endian formatted byte array to its long value.
     * Byte array must be {@value Long#BYTES} or less in length.
     *
     * @param bytes Big-endian formatted byte array
     * @return Corresponding long value
     * @throws C3rRuntimeException If the byte array is more than the max length
     */
    static Long longFromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        } else if (bytes.length > Long.BYTES) {
            throw new C3rRuntimeException("Long values must be " + Long.BYTES + " bytes or less.");
        }
        return new BigInteger(bytes).longValue();
    }

    /**
     * Convert a long value to its big-endian byte representation.
     *
     * @param value Long
     * @return Big-endian byte encoding of value
     */
    static byte[] longToBytes(final Long value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * Converts big-endian formatted bytes to float value.
     * Number of bytes must be {@value Float#BYTES}.
     *
     * @param bytes Bytes in big-endian format
     * @return Corresponding float value
     * @throws C3rRuntimeException If the byte array is not the expected length
     */
    static Float floatFromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        } else if (bytes.length != Float.BYTES) {
            throw new C3rRuntimeException("Float values may only be " + Float.BYTES + " bytes long.");
        }
        return ByteBuffer.wrap(bytes).getFloat();
    }

    /**
     * Convert a float value to its big-endian byte representation.
     *
     * @param value Float
     * @return Big-endian encoding of value
     */
    static byte[] floatToBytes(final Float value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
    }

    /**
     * Converts a big-endian formatted byte array to its double value.
     * Number of bytes must be {@value Double#BYTES}.
     *
     * @param bytes Bytes in big-endian format
     * @return Corresponding float value
     * @throws C3rRuntimeException If the byte array is not the expected length
     */
    static Double doubleFromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        } else if (bytes.length != Double.BYTES) {
            throw new C3rRuntimeException("Double values may only be " + Double.BYTES + " bytes long.");
        }
        return ByteBuffer.wrap(bytes).getDouble();
    }

    /**
     * Convert a double value to its big-endian byte representation.
     *
     * @param value Double
     * @return Big-endian encoding of value
     */
    static byte[] doubleToBytes(final Double value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
    }

    /**
     * Encode a value as bytes.
     *
     * @return The underlying byte[]
     */
    public abstract byte[] getBytes();

    /**
     * Length of the value when byte encoded.
     *
     * @return The length of the underlying byte[]
     */
    public abstract int byteLength();
}
