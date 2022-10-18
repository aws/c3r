// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import edu.umd.cs.findbugs.annotations.UnknownNullness;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Implementation of {@link Value} for the Parquet data format.
 */
@EqualsAndHashCode(callSuper = false)
public abstract class ParquetValue extends Value {
    /**
     * What the deserialized data type is.
     */
    @Getter
    private final ParquetDataType parquetDataType;

    /**
     * Byte array encoding of the value. Uses big-endian format for numerical values.
     */
    private final byte[] bytes;

    /**
     * Associates a data type with a binary encoded value.
     *
     * @param parquetDataType Underlying data type
     * @param bytes           Binary serialization of value
     */
    protected ParquetValue(@NonNull final ParquetDataType parquetDataType, final byte[] bytes) {
        this.parquetDataType = parquetDataType;
        this.bytes = bytes;
    }

    /**
     * Associates a data type with a binary encoded value.
     *
     * @param type  Underlying data type
     * @param bytes Binary serialization of value
     * @return Binary value with metadata
     * @throws C3rIllegalArgumentException If the data type is not supported
     */
    public static ParquetValue fromBytes(final ParquetDataType type, final byte[] bytes) {
        if (!ParquetDataType.isSupportedType(type.getParquetType())) {
            throw new C3rIllegalArgumentException("Unsupported parquet type: " + type.getParquetType());
        }
        // asPrimitiveType() is guaranteed to work here because ParquetDataType.isSupportedType only
        // returns true for a subset of primitive types

        switch (type.getParquetType().asPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new Boolean(type, booleanFromBytes(bytes));
            case INT32:
                return new Int(type, intFromBytes(bytes));
            case INT64:
                return new Long(type, longFromBytes(bytes));
            case FLOAT:
                return new Float(type, floatFromBytes(bytes));
            case DOUBLE:
                return new Double(type, doubleFromBytes(bytes));
            case BINARY:
                org.apache.parquet.io.api.Binary binary = null;
                if (bytes != null) {
                    binary = org.apache.parquet.io.api.Binary.fromReusedByteArray(bytes);
                }
                return new Binary(type, binary);
            default:
                throw new C3rIllegalArgumentException("Unrecognized data type: " + type);
        }
    }

    /**
     * Get a copy of the binary value.
     *
     * @return Copy of binary data
     */
    public byte[] getBytes() {
        if (bytes != null) {
            return this.bytes.clone();
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int byteLength() {
        if (bytes != null) {
            return this.bytes.length;
        } else {
            return 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull() {
        return bytes == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClientDataType getClientDataType() {
        return parquetDataType.getClientDataType();
    }

    /**
     * Write value to specified output.
     *
     * @param consumer Output location
     */
    public abstract void writeValue(RecordConsumer consumer);

    /**
     * Specific implementation for binary Parquet values.
     */
    @Getter
    public static class Binary extends ParquetValue {
        /**
         * Binary value.
         */
        private final org.apache.parquet.io.api.Binary value;

        /**
         * Convert a Parquet binary value to its byte representation with data type metadata.
         * This constructor takes the {@link ParquetDataType} as it contains information about
         * the binary encoding such as if it's a string.
         *
         * @param type  The binary type information
         * @param value Binary value to store
         * @throws C3rRuntimeException If a data type other than binary found
         */
        public Binary(final ParquetDataType type, final org.apache.parquet.io.api.Binary value) {
            super(type, binaryToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.BINARY, type)) {
                throw new C3rRuntimeException("Parquet Binary type expected but found " + type);
            }
            this.value = value != null ? value.copy() : null;
        }

        /**
         * Convert a Parquet binary value to an array of bytes.
         *
         * @param value Parquet Binary
         * @return Byte representation of value
         */
        private static byte[] binaryToBytes(final org.apache.parquet.io.api.Binary value) {
            if (value == null) {
                return null;
            }
            return value.getBytes().clone();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addBinary(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            if (this.getParquetDataType().isStringType()) {
                return value.toStringUsingUTF8();
            } else {
                return value.toString();
            }
        }
    }

    /**
     * Specific implementation for boolean Parquet values.
     */
    public static class Boolean extends ParquetValue {
        /**
         * Boolean value.
         */
        private final java.lang.Boolean value;

        /**
         * Convert a boolean value to its byte representation with data type metadata.
         *
         * @param parquetDataType The boolean type information
         * @param value           Boolean to store
         * @throws C3rRuntimeException If a data type other than boolean found
         */
        public Boolean(final ParquetDataType parquetDataType, final java.lang.Boolean value) {
            super(parquetDataType, booleanToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.BOOLEAN, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Boolean type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * Get stored value.
         *
         * @return {@code true}, {@code false} or {@code null}
         */
        public java.lang.Boolean getValue() {
            return value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addBoolean(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            return String.valueOf(value);
        }

    }

    /**
     * Specific implementation for double Parquet values.
     */
    @Getter
    public static class Double extends ParquetValue {
        /**
         * Double value.
         */
        private final java.lang.Double value;

        /**
         * Convert a double value to its byte representation with data type metadata.
         *
         * @param parquetDataType The double type information
         * @param value           Double to store
         * @throws C3rRuntimeException If a data type other than double found
         */
        public Double(final ParquetDataType parquetDataType, final java.lang.Double value) {
            super(parquetDataType, doubleToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.DOUBLE, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Double type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addDouble(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            return String.valueOf(value);
        }
    }


    /**
     * Specific implementation for float Parquet values.
     */
    @Getter
    public static class Float extends ParquetValue {
        /**
         * Float value.
         */
        private final java.lang.Float value;

        /**
         * Convert a double value to its byte representation with data type metadata.
         *
         * @param parquetDataType The float type information
         * @param value           Float to store
         * @throws C3rRuntimeException If a data type other than float found
         */
        public Float(final ParquetDataType parquetDataType, final java.lang.Float value) {
            super(parquetDataType, floatToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.FLOAT, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Float type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addFloat(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            return String.valueOf(value);
        }

    }

    /**
     * Specific implementation for integer Parquet values.
     */
    @Getter
    public static class Int extends ParquetValue {
        /**
         * Integer value.
         */
        private final java.lang.Integer value;

        /**
         * Convert an integer value to its byte representation with data type metadata.
         *
         * @param parquetDataType The integer type data
         * @param value           Integer to store
         * @throws C3rRuntimeException If a data type other than integer found
         */
        public Int(final ParquetDataType parquetDataType, final java.lang.Integer value) {
            super(parquetDataType, intToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.INT32, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Integer type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addInteger(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            return String.valueOf(value);
        }
    }

    /**
     * Specific implementation for long Parquet values.
     */
    @Getter
    public static class Long extends ParquetValue {
        /**
         * Long value.
         */
        private final java.lang.Long value;

        /**
         * Convert a long value to its byte representation with data type metadata.
         *
         * @param parquetDataType The long type data
         * @param value           Long to store
         * @throws C3rRuntimeException If a data type other than long found
         */
        public Long(final ParquetDataType parquetDataType, final java.lang.Long value) {
            super(parquetDataType, longToBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.INT64, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Long type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final RecordConsumer consumer) {
            if (value != null) {
                consumer.addLong(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @UnknownNullness
        public String toString() {
            if (isNull()) {
                return null;
            }
            return String.valueOf(value);
        }
    }

    /**
     * Checks that the actual Parquet value is a primitive type and is the type that is expected by a particular constructor.
     *
     * @param expected Type expected by constructor
     * @param actual   Type passed into constructor
     * @return If {@code expected} and {@code actual} match
     */
    static boolean isExpectedType(final PrimitiveType.PrimitiveTypeName expected, final ParquetDataType actual) {
        if (!actual.getParquetType().isPrimitive()) {
            return false;
        }

        return expected == actual.getParquetType().asPrimitiveType().getPrimitiveTypeName();
    }
}