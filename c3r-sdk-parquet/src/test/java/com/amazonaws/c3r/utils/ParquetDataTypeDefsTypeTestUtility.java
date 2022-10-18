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
public final class ParquetDataTypeDefsTypeTestUtility {
    /**
     * Zero or one instances of a binary subtype string value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_STRING_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("STRING");

    /**
     * Exactly one instance of a binary subtype string value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_STRING_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("STRING");

    /**
     * Unsupported primitive type (repeated: 0, 1 or more instances required per row group) for testing purposes.
     */
    public static final org.apache.parquet.schema.Type REPEATED_STRING_TYPE =
            Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("STRING");

    /**
     * Exactly one instance of a binary value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_BINARY_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .named("BINARY");

    /**
     * Zero or one instances of a boolean value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_BOOLEAN_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("BOOLEAN");

    /**
     * Exactly one instance of a boolean value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_BOOLEAN_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("BOOLEAN");

    /**
     * Zero or one instances of a double value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_DOUBLE_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                    .named("DOUBLE");

    /**
     * Exactly one instance of a double value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_DOUBLE_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                    .named("DOUBLE");

    /**
     * Zero or one instances of a float value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_FLOAT_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .named("FLOAT");

    /**
     * Exactly one instance of a float value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_FLOAT_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .named("FLOAT");

    /**
     * Zero or one instances of an int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT32_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("INT32");

    /**
     * Exactly one instance of an int/int32 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT32_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("INT32");

    /**
     * Zero or one instances of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type OPTIONAL_INT64_TYPE =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("INT64");

    /**
     * Exactly one instance of a long/int64 value is required per row group.
     */
    public static final org.apache.parquet.schema.Type REQUIRED_INT64_TYPE =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("INT64");

    /**
     * Unsupported logical type for testing purposes.
     */
    public static final org.apache.parquet.schema.Type COMPLEX_TYPE =
            Types.requiredMap()
                    .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                    .optionalValue(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("ZipMap");

    /**
     * Hidden utility constructor.
     */
    private ParquetDataTypeDefsTypeTestUtility() {
    }
}
