// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Validatable;
import lombok.Value;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Supported Parquet types.
 */
@Value
public final class ParquetDataType implements Validatable {
    /**
     * This is the Parquet type used for nonces.
     */
    private static final org.apache.parquet.schema.Type NONCE_PARQUET_TYPE = org.apache.parquet.schema.Types
            .required(PrimitiveTypeName.BINARY).named("nonce");

    /**
     * The type used for nonces in ParquetDataTypes.
     */
    public static final ParquetDataType NONCE_TYPE = new ParquetDataType(NONCE_PARQUET_TYPE, ClientDataType.UNKNOWN);

    /**
     * Client type.
     */
    private ClientDataType clientDataType;

    /**
     * Parquet type.
     */
    private org.apache.parquet.schema.Type parquetType;

    /**
     * Private constructor to set up the nonce data type correctly.
     *
     * @param pType Parquet type
     * @param cType C3r type
     */
    private ParquetDataType(final org.apache.parquet.schema.Type pType, final ClientDataType cType) {
        parquetType = pType;
        clientDataType = cType;
        validate();
    }

    /**
     * Checks if the Parquet type is supported by C3R.
     * Must be a primitive type that is not an {@code INT96}.
     *
     * @param type Parquet type
     * @return {@code true} if supported
     */
    public static boolean isSupportedType(final org.apache.parquet.schema.Type type) {
        return type.isPrimitive()
                && type.getRepetition() != Type.Repetition.REPEATED
                && type.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT96
                && getClientDataType(type) != ClientDataType.UNKNOWN;
    }

    /**
     * Convert a Parquet schema type to a C3R Parquet data type after verifying compatibility.
     *
     * @param type Parquet type associated with a column
     * @return Instance of the associated {@code ParquetDataTy[e}
     * @throws C3rIllegalArgumentException If the data type is an unsupported Parquet data type
     */
    public static ParquetDataType fromType(final org.apache.parquet.schema.Type type) {
        if (!ParquetDataType.isSupportedType(type)) {
            return new ParquetDataType(type, ClientDataType.UNKNOWN);
        }
        return new ParquetDataType(type, getClientDataType(type));
    }

    /**
     * Checks if this is a string.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a string
     */
    static boolean isStringType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.BINARY) &&
            annotation != null && annotation.equals(LogicalTypeAnnotation.stringType());
    }

    /**
     * Checks if this is a plain int64 value. The int64 type can either have no logical annotations or the {@code INT}
     * annotation with the {@code bitWidth} parameter set to 64 and {@code isSigned} set to {@code true}.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a signed {@code int64} value
     */
    static boolean isBigIntType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT64)
                && (annotation == null
                    || type.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(ClientDataType.BIGINT_BIT_SIZE, true)));
    }

    /**
     * Checks if this is a plain boolean value. There should be no annotations on the type.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a {@code boolean} value
     */
    static boolean isBooleanType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.BOOLEAN)
                && annotation == null;
    }

    /**
     * Checks if this is a date value. The primitive data type must be an {@code int32} and the logical annotation must
     * be {@code Date}.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a {@code Date} value
     */
    static boolean isDateType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT32)
                && annotation != null
                && annotation.equals(LogicalTypeAnnotation.dateType());
    }

    /**
     * Checks if this is a decimal value. Decimal is a parameterized type ({@code Decimal(scale, precision)}). This function does not check
     * whether or not a particular scale or precisions is being used, just that the logical annotation is an instance of Decimal. Decimal
     * is also backed by one of four different data structures in Parquet. We ensure that the primitive data type is one of the four
     * valid options but do not check for a specific primitive type.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a {@code Decimal} value
     */
    static boolean isDecimalType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        // Multiple primitive types can be used to store DECIMAL values depending on the precision
        if ((pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT32)
                || pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT64)
                || pt.getPrimitiveTypeName().equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                || pt.getPrimitiveTypeName().equals(PrimitiveTypeName.BINARY)) && type.getLogicalTypeAnnotation() != null) {
            /*
             * Because the decimal annotation is a parameterized type (LogicalTypeAnnotation.decimalType(scale, precision)),
             * we can't do a direct equality check of the type name since the scale and precision information isn't known here:
             *   type.getLogicalAnnotation().equals(LogicalTypeAnnotation.decimalType(scale, precision)
             * Regardless of scale and precision, all are an instance of the DecimalLogicalTypeAnnotation class so we use:
             *   type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
             */
            return annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
        }
        return false;
    }

    /**
     * Checks if this is a plain double value. The double type should not have any logical annotations on it.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a {@code double} value
     */
    static boolean isDoubleType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.DOUBLE)
                && annotation == null;
    }

    /**
     * Checks if this is a plain float value. The float type should not have any logical annotations on it.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a {@code float} value
     */
    static boolean isFloatType(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.FLOAT)
                && annotation == null;
    }

    /**
     * Checks if this is a plain int32 value. The int32 type can either have no logical annotations or the {@code INT}
     * annotation with the {@code bitWidth} parameter set to 32 and {@code isSigned} set to {@code true}.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a signed {@code int32} value
     */
    static boolean isInt32Type(final org.apache.parquet.schema.Type type) {
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT32)
                && (annotation == null || annotation.equals(LogicalTypeAnnotation.intType(ClientDataType.INT_BIT_SIZE, true)));
    }

    /**
     * Checks if this is a plain smallint value. The smallint is contained inside the primitive type int32 with the {@code INT} logical
     * annotation where {@code bitWidth} is 16 and {@code isSigned}  is true.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a signed {@code smallint} value
     */
    static boolean isSmallIntType(final org.apache.parquet.schema.Type type) {
        // A short value (16 bits) is stored in the primitive type INT32 and annotated with a specific bit width and
        // flag indicating if it is a signed or unsigned integer
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT32)
                && annotation != null
                && annotation.equals(LogicalTypeAnnotation.intType(ClientDataType.SMALLINT_BIT_SIZE, true));
    }

    /**
     * Checks if this is a timestamp value. The timestamp type is backed by the int64 primitive and has the logical type
     * {@code Timestamp(isAdjustedToUTC, timeUnit)}. This function checks to make sure the timestamp annotation is there
     * and does not check for a specific time unit being used or if the time is in UTC or not.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a signed {@code timestamp} value
     */
    static boolean isTimestampType(final org.apache.parquet.schema.Type type) {
        /*
         * Because the timestamp annotation is a parameterized type (LogicalTypeAnnotation.timestampType(isAdjustedToUTC, timeUnit)),
         * we can't do a direct equality check of the type name since the UTC adjustment and unit information isn't known here:
         *   type.getLogicalAnnotation().equals(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, timeUnit))
         * Regardless of the UTC adjustment and unit, all are an instance of the TimestampLogicalTypeAnnotation class so we use:
         *   type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
         */
        if (!type.isPrimitive()) {
            return false;
        }
        final PrimitiveType pt = type.asPrimitiveType();
        final LogicalTypeAnnotation annotation = pt.getLogicalTypeAnnotation();
        return pt.getPrimitiveTypeName().equals(PrimitiveTypeName.INT64)
                && annotation != null
                && annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
    }

    /**
     * Checks if this is a fixed-width byte array.
     *
     * @param type Parquet Type
     * @return If the type represents a fixed width byte array
     */
    static boolean isCharType(final org.apache.parquet.schema.Type type) {
        return false;
    }

    /**
     * Convert the Parquet data type to the equivalent C3R data type.
     *
     * @param type Parquet type
     * @return C3R data type
     */
    private static ClientDataType getClientDataType(final org.apache.parquet.schema.Type type) {
        if (type.isPrimitive()) {
            if (isStringType(type)) {
                return ClientDataType.STRING;
            } else if (isInt32Type(type)) {
                return ClientDataType.INT;
            } else if (isBigIntType(type)) {
                return ClientDataType.BIGINT;
            } else if (isSmallIntType(type)) {
                return ClientDataType.SMALLINT;
            } else if (isDecimalType(type)) {
                return ClientDataType.DECIMAL;
            } else if (isDateType(type)) {
                return ClientDataType.DATE;
            } else if (isDoubleType(type)) {
                return ClientDataType.DOUBLE;
            } else if (isFloatType(type)) {
                return ClientDataType.FLOAT;
            } else if (isBooleanType(type)) {
                return ClientDataType.BOOLEAN;
            } else if (isTimestampType(type)) {
                return ClientDataType.TIMESTAMP;
            } else if (isCharType(type)) {
                return ClientDataType.CHAR;
            } else {
                return ClientDataType.UNKNOWN;
            }
        }
        return ClientDataType.UNKNOWN;
    }

    /**
     * Creates a Parquet type with correct metadata including specified name.
     *
     * @param name Label to use for this particular type
     * @return A copy of the Parquet type with the specified name
     * @throws C3rIllegalArgumentException If the data type is an unsupported Parquet data type
     */
    public org.apache.parquet.schema.Type toTypeWithName(final String name) {
        final var primType = parquetType.asPrimitiveType().getPrimitiveTypeName();
        final var logicalAnn = parquetType.getLogicalTypeAnnotation();
        switch (parquetType.getRepetition()) {
            case OPTIONAL:
                return Types.optional(primType).as(logicalAnn).named(name);
            case REQUIRED:
                return Types.required(primType).as(logicalAnn).named(name);
            default:
                throw new C3rIllegalArgumentException("Unsupported parquet type: " + parquetType + ".");
        }
    }

    @Override
    public void validate() {
        if (parquetType == null) {
            return;
        }
        if (clientDataType == ClientDataType.UNKNOWN) {
            return;
        }
        if (!isSupportedType(parquetType)) {
            throw new C3rRuntimeException("Parquet type " + parquetType + " is not supported.");
        } else if (parquetType.isPrimitive()) {
            final PrimitiveTypeName name = parquetType.asPrimitiveType().getPrimitiveTypeName();
            if (name == PrimitiveTypeName.INT32 || name == PrimitiveTypeName.INT64) {
                final LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
                if (logicalTypeAnnotation == null) {
                    return;
                }
                if (logicalTypeAnnotation.equals(LogicalTypeAnnotation.intType(ClientDataType.SMALLINT_BIT_SIZE, false)) ||
                        logicalTypeAnnotation.equals(LogicalTypeAnnotation.intType(ClientDataType.INT_BIT_SIZE, false)) ||
                        logicalTypeAnnotation.equals(LogicalTypeAnnotation.intType(ClientDataType.BIGINT_BIT_SIZE, false))) {
                    throw new C3rRuntimeException("Unsigned integer values are not supported.");
                }
            }
        }
    }
}
