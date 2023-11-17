// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

/**
 * Contains definitions for each primitive data type as both logical and required. An unsupported logical type
 * and an unsupported repeated primitive type are included for testing purposes.
 */
public final class ParquetTypeDefsTestUtility {
    /**
     * Length of the fixed width arrays.
     */
    public static final int FIXED_WIDTH_LENGTH = 5;

    /**
     * Optional fixed length byte array.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_FIXED_LEN_BYTE_ARRAY =
            Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(FIXED_WIDTH_LENGTH)
                    .named("OPT_FIXED_LEN_BYTE_ARRAY");

    /**
     * Required fixed length byte array.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_FIXED_LEN_BYTE_ARRAY =
            Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(FIXED_WIDTH_LENGTH)
                    .named("REQ_FIXED_LEN_BYTE_ARRAY");

    /**
     * Zero or one instances of a binary subtype string value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_STRING_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("OPT_STRING");

    /**
     * Exactly one instance of a binary subtype string value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_STRING_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("REQ_STRING");

    /**
     * Unsupported primitive type (repeated: 0, 1 or more instances required per row group) for testing purposes.
     */
    public static final org.apache.parquet.schema.Type REPEATED_STRING_TYPE =
            Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("REPEAT_STRING");

    /**
     * Exactly one instance of a binary value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_BINARY_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .named("REQ_BINARY");

    /**
     * Zero or one instances of a boolean value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_BOOLEAN_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("OPT_BOOLEAN");

    /**
     * Exactly one instance of a boolean value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_BOOLEAN_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("REQ_BOOLEAN");

    /**
     * Zero or one instances of a double value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_DOUBLE_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                    .named("OPT_DOUBLE");

    /**
     * Exactly one instance of a double value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_DOUBLE_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                    .named("REQ_DOUBLE");

    /**
     * Zero or one instances of a float value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_FLOAT_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .named("OPT_FLOAT");

    /**
     * Exactly one instance of a float value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_FLOAT_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .named("REQ_FLOAT");

    /**
     * Exactly one instance of an 8-bit integer is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT8_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(8, true))
                    .named("OPT_INT8");

    /**
     * Zero or one instances of a signed int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT32_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("OPT_INT32");

    /**
     * Zero or one instances of a signed int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT32_ANNOTATED_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32, true))
                    .named("OPT_ANNOTATED_INT32");

    /**
     * Exactly one instance of an int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT32_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("REQ_INT32");

    /**
     * Exactly one instance of an int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT32_ANNOTATED_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32, true))
                    .named("REQ_ANNOTATED_INT32");

    /**
     * Zero or one instances of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT64_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("OPT_INT64");

    /**
     * Zero or one instances of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT64_ANNOTATED_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.intType(64, true))
                    .named("OPT_ANNOTATED_INT64");

    /**
     * Exactly one instance of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT64_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("REQ_INT64");

    /**
     * Exactly one instance of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT64_ANNOTATED_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.intType(64, true))
                    .named("REQ_ANNOTATED_INT64");

    public static final org.apache.parquet.schema.Type OPTIONAL_DATE_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.dateType())
                    .named("OPT_DATE");

    public static final org.apache.parquet.schema.Type REQUIRED_DATE_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.dateType())
                    .named("REQ_DATE");

    public static final org.apache.parquet.schema.Type OPTIONAL_INT16_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(16, true))
                    .named("OPT_INT16");

    public static final org.apache.parquet.schema.Type REQUIRED_INT16_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(16, true))
                    .named("REQ_INT16");

    public static final org.apache.parquet.schema.Type OPTIONAL_TIMESTAMP_UTC_NANO_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .named("OPT_UTC_NANO_TIMESTAMP");

    public static final org.apache.parquet.schema.Type REQUIRED_TIMESTAMP_UTC_NANO_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .named("REQ_UTC_NANOTIMESTAMP");

    public static final org.apache.parquet.schema.Type REQUIRED_TIMESTAMP_NANO_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .named("REQ_NANO_TIMESTAMP");

    /**
     * Unsupported logical type for testing purposes.
     */
    public static final org.apache.parquet.schema.Type COMPLEX_TYPE =
            Types.requiredMap()
                    .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .optionalValue(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("ZipMap");

    /**
     * Unsupported unsigned int8 value for testing purposes.
     */
    public static final org.apache.parquet.schema.Type UNSIGNED_INT8_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(8, false))
                    .named("UINT8");

    /**
     * Unsupported unsigned int16 value for testing purposes.
     */
    public static final org.apache.parquet.schema.Type UNSIGNED_INT16_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(16, false))
                    .named("UINT16");

    /**
     * Unsupported unsigned int32 value for testing purposes.
     */
    public static final org.apache.parquet.schema.Type UNSIGNED_INT32_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32, false))
                    .named("UINT32");

    /**
     * Unsupported unsigned int64 value for testing purposes.
     */
    public static final org.apache.parquet.schema.Type UNSIGNED_INT64_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.intType(64, false))
                    .named("UINT64");

    /**
     * Fixed length byte array decimal.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_BYTE_ARRAY_DECIMAL_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(32)
                    .as(LogicalTypeAnnotation.decimalType(16, 16))
                    .named("REQ_DECIMAL_10_2");

    /**
     * Fixed length byte array decimal.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_BYTE_ARRAY_DECIMAL_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(32)
                    .as(LogicalTypeAnnotation.decimalType(16, 16))
                    .named("REQ_DECIMAL_10_2");

    /**
     * Hidden utility constructor.
     */
    private ParquetTypeDefsTestUtility() {
    }
}
