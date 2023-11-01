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
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
     * @throws C3rIllegalArgumentException Type not supported
     */
    ParquetValue(@NonNull final ParquetDataType parquetDataType, final byte[] bytes) {
        this.parquetDataType = parquetDataType;
        this.bytes = bytes;
        if (!validateAnnotation()) {
            throw new C3rIllegalArgumentException("Parquet " + parquetDataType.getParquetType() + " type has invalid logical type " +
                    "annotations.");
        }
    }

    /**
     * Verify annotations on Parquet type are allowed on this primitive type.
     *
     * @return {@code true} if annotations are accepted on type
     */
    abstract boolean validateAnnotation();

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
                return new Boolean(type, ValueConverter.Boolean.fromBytes(bytes));
            case INT32:
                return new Int32(type, ValueConverter.Int.fromBytes(bytes));
            case INT64:
                return new Int64(type, ValueConverter.BigInt.fromBytes(bytes));
            case FLOAT:
                return new Float(type, ValueConverter.Float.fromBytes(bytes));
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
                return new Binary(type, (bytes != null) ? org.apache.parquet.io.api.Binary.fromReusedByteArray(bytes) : null);
            case DOUBLE:
                return new Double(type, ValueConverter.Double.fromBytes(bytes));
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
         * If this instance of {@code Binary} represents a {@code FIXED_WIDTH_BYTE_ARRAY} a length will be specified.
         */
        private final int length;

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
            if (!(isExpectedType(PrimitiveType.PrimitiveTypeName.BINARY, type) ||
                    isExpectedType(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, type))) {
                throw new C3rRuntimeException("Parquet Binary type expected but found " + type);
            }
            this.value = (value != null) ? value.copy() : null;
            this.length = (value != null) ? value.length() : 0;
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
         * The {@code Binary} Parquet type can represent the primitive type and the logical types {@code String} and {@code Decimal}.
         *
         * @return {@code true} if instance is a raw primitive or valid logical type
         */
        @Override
        boolean validateAnnotation() {
            if (getParquetDataType().getParquetType().asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                final LogicalTypeAnnotation annotations = getParquetDataType().getParquetType().getLogicalTypeAnnotation();
                return (annotations == null) ||
                        (annotations instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) ||
                        (annotations instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation);
            } else if (getParquetDataType().getParquetType().asPrimitiveType().getPrimitiveTypeName() ==
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                final LogicalTypeAnnotation annotations = getParquetDataType().getParquetType().getLogicalTypeAnnotation();
                if (getParquetDataType().getParquetType().asPrimitiveType().getTypeLength() < 0) {
                    return false;
                } else {
                    return (annotations == null) ||
                            (annotations instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation);
                }
            }
            return false;
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
            if (ClientDataType.STRING == getParquetDataType().getClientDataType() ||
                    ClientDataType.CHAR == getParquetDataType().getClientDataType()) {
                return value.toStringUsingUTF8();
            } else {
                return value.toString();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                case STRING:
                    if (getParquetDataType().getClientDataType() == ClientDataType.STRING) {
                        return getBytes();
                    } else {
                        throw new C3rRuntimeException("Could not convert Parquet Binary to " + type + ".");
                    }
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Binary to " + type + ".");
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
            super(parquetDataType, ValueConverter.Boolean.toBytes(value));
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
         * The {@code Boolean} Parquet type cannot have any logical type annotations.
         *
         * @return {@code true} if no logical type annotations exist
         */
        @Override
        boolean validateAnnotation() {
            return getParquetDataType().getParquetType().getLogicalTypeAnnotation() == null;
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

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                case BOOLEAN:
                    return getBytes();
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Boolean to " + type + ".");
            }
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
            super(parquetDataType, ValueConverter.Double.toBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.DOUBLE, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Double type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * The {@code Double} Parquet type cannot have any logical type annotations.
         *
         * @return {@code true} if no logical type annotations exist
         */
        @Override
        boolean validateAnnotation() {
            return getParquetDataType().getParquetType().getLogicalTypeAnnotation() == null;
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

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Double to " + type + ".");
            }
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
            super(parquetDataType, ValueConverter.Float.toBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.FLOAT, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Float type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * The {@code Float} Parquet type cannot have any logical type annotations.
         *
         * @return {@code true} if no logical type annotations exist
         */
        @Override
        boolean validateAnnotation() {
            return getParquetDataType().getParquetType().getLogicalTypeAnnotation() == null;
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

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Float to " + type + ".");
            }
        }
    }

    /**
     * Specific implementation for integer Parquet values.
     */
    @Getter
    public static class Int32 extends ParquetValue {
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
        public Int32(final ParquetDataType parquetDataType, final java.lang.Integer value) {
            super(parquetDataType, ValueConverter.Int.toBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.INT32, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Integer type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * The {@code int32} Parquet value can have no annotations or have the logical type annotations of {@code Date}, {@code Decimal}
         * or {@code Int32}.
         *
         * @return {@code true} if there is no annotation or the annotation is for Date, Decimal or Int32.
         */
        @Override
        boolean validateAnnotation() {
            final LogicalTypeAnnotation logicalTypeAnnotation = getParquetDataType().getParquetType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation != null && logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalTypeAnnotation =
                        (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalTypeAnnotation;
                return intLogicalTypeAnnotation.isSigned() &&
                        (intLogicalTypeAnnotation.getBitWidth() == Byte.SIZE ||
                                intLogicalTypeAnnotation.getBitWidth() == ClientDataType.INT_BIT_SIZE ||
                                intLogicalTypeAnnotation.getBitWidth() == ClientDataType.SMALLINT_BIT_SIZE);
            }
            return (logicalTypeAnnotation == null) ||
                    (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) ||
                    (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation);
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

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                case BIGINT:
                    return ValueConverter.BigInt.toBytes(value);
                case DATE:
                    return ValueConverter.Date.toBytes(value);
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Int32 to " + type + ".");
            }
        }
    }

    /**
     * Specific implementation for long Parquet values.
     */
    @Getter
    public static class Int64 extends ParquetValue {
        /**
         * Int64 value.
         */
        private final java.lang.Long value;

        /**
         * Convert a long value to its byte representation with data type metadata.
         *
         * @param parquetDataType The long type data
         * @param value           Int64 to store
         * @throws C3rRuntimeException If a data type other than long found
         */
        public Int64(final ParquetDataType parquetDataType, final java.lang.Long value) {
            super(parquetDataType, ValueConverter.BigInt.toBytes(value));
            if (!isExpectedType(PrimitiveType.PrimitiveTypeName.INT64, parquetDataType)) {
                throw new C3rRuntimeException("Parquet Int64 type expected but found " + parquetDataType);
            }
            this.value = value;
        }

        /**
         * The {@code int64} Parquet type can have no annotations or the {@code Timestamp}, {@code Decimal} or {@code Int32} annotations.
         *
         * @return {@code true} if no annotations exist or the annotation is timestamp, decimal or int.
         */
        @Override
        boolean validateAnnotation() {
            final LogicalTypeAnnotation logicalTypeAnnotation = getParquetDataType().getParquetType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalTypeAnnotation =
                        (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalTypeAnnotation;
                return intLogicalTypeAnnotation.isSigned() && intLogicalTypeAnnotation.getBitWidth() == ClientDataType.BIGINT_BIT_SIZE;
            }
            return logicalTypeAnnotation == null ||
                    logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ||
                    logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
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

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] getBytesAs(final ClientDataType type) {
            switch (type) {
                case BIGINT:
                    return ValueConverter.BigInt.toBytes(value);
                default:
                    throw new C3rRuntimeException("Could not convert Parquet Int64 to " + type + ".");
            }
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
