// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.data.ClientDataType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

/**
 * Contains definitions for each primitive data type as both logical and required. An unsupported logical type
 * and an unsupported repeated primitive type are included for testing purposes.
 */
public final class ParquetTypeDefsTestUtility {
    public static final List<Type> ALL_SUPPORTED_TYPES = List.of(
            SupportedTypes.OPTIONAL_BINARY_DECIMAL_TYPE, SupportedTypes.REQUIRED_BINARY_DECIMAL_TYPE,
            SupportedTypes.OPTIONAL_BINARY_STRING_TYPE, SupportedTypes.REQUIRED_BINARY_STRING_TYPE,
            SupportedTypes.OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE, SupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE,
            SupportedTypes.OPTIONAL_BOOLEAN_TYPE, SupportedTypes.REQUIRED_BOOLEAN_TYPE,
            SupportedTypes.OPTIONAL_DOUBLE_TYPE, SupportedTypes.REQUIRED_DOUBLE_TYPE,
            SupportedTypes.OPTIONAL_FLOAT_TYPE, SupportedTypes.REQUIRED_FLOAT_TYPE,
            SupportedTypes.OPTIONAL_INT32_TYPE, SupportedTypes.REQUIRED_INT32_TYPE,
            SupportedTypes.OPTIONAL_INT32_INT_32_TRUE_TYPE, SupportedTypes.REQUIRED_INT32_INT_32_TRUE_TYPE,
            SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE, SupportedTypes.REQUIRED_INT32_INT_16_TRUE_TYPE,
            SupportedTypes.OPTIONAL_INT32_DATE_TYPE, SupportedTypes.REQUIRED_INT32_DATE_TYPE,
            SupportedTypes.OPTIONAL_INT32_DECIMAL_TYPE, SupportedTypes.REQUIRED_INT32_DECIMAL_TYPE,
            SupportedTypes.OPTIONAL_INT64_TYPE, SupportedTypes.REQUIRED_INT64_TYPE,
            SupportedTypes.OPTIONAL_INT64_INT_64_TRUE_TYPE, SupportedTypes.REQUIRED_INT64_INT_64_TRUE_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE,
            SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE,
            SupportedTypes.OPTIONAL_INT64_DECIMAL_TYPE, SupportedTypes.REQUIRED_INT64_DECIMAL_TYPE
    );

    public static final List<Type> ALL_UNSUPPORTED_TYPES = List.of(
            UnsupportedTypes.OPTIONAL_BINARY_TYPE, UnsupportedTypes.REQUIRED_BINARY_TYPE,
            UnsupportedTypes.OPTIONAL_FIXED_LEN_BYTE_ARRAY_TYPE, UnsupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_TYPE,
            UnsupportedTypes.OPTIONAL_BINARY_ENUM_TYPE, UnsupportedTypes.REQUIRED_BINARY_ENUM_TYPE,
            UnsupportedTypes.OPTIONAL_FIXED_LEN_BYTE_ARRAY_UUID_TYPE, UnsupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_UUID_TYPE,
            UnsupportedTypes.OPTIONAL_BINARY_JSON_TYPE, UnsupportedTypes.REQUIRED_BINARY_JSON_TYPE,
            UnsupportedTypes.OPTIONAL_BINARY_BSON_TYPE, UnsupportedTypes.REQUIRED_BINARY_BSON_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_INT_8_TRUE_TYPE, UnsupportedTypes.REQUIRED_INT32_INT_8_TRUE_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_INT_8_FALSE_TYPE, UnsupportedTypes.REQUIRED_INT32_INT_8_FALSE_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_INT_16_FALSE_TYPE, UnsupportedTypes.REQUIRED_INT32_INT_16_FALSE_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_INT_32_FALSE_TYPE, UnsupportedTypes.REQUIRED_INT32_INT_32_FALSE_TYPE,
            UnsupportedTypes.OPTIONAL_INT64_INT_64_FALSE_TYPE, UnsupportedTypes.REQUIRED_INT64_INT_64_FALSE_TYPE,
            UnsupportedTypes.OPTIONAL_INT64_TIME_UTC_NANOS_TYPE, UnsupportedTypes.REQUIRED_INT64_TIME_UTC_NANOS_TYPE,
            UnsupportedTypes.OPTIONAL_INT64_TIME_NOT_UTC_NANOS_TYPE, UnsupportedTypes.REQUIRED_INT64_TIME_NOT_UTC_NANOS_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_TIME_UTC_MILLIS_TYPE, UnsupportedTypes.REQUIRED_INT32_TIME_UTC_MILLIS_TYPE,
            UnsupportedTypes.OPTIONAL_INT32_TIME_NOT_UTC_MILLIS_TYPE, UnsupportedTypes.REQUIRED_INT32_TIME_NOT_UTC_MILLIS_TYPE,
            UnsupportedTypes.OPTIONAL_INT64_TIME_UTC_MICROS_TYPE, UnsupportedTypes.REQUIRED_INT64_TIME_UTC_MICROS_TYPE,
            UnsupportedTypes.OPTIONAL_INT64_TIME_NOT_UTC_MICROS_TYPE, UnsupportedTypes.REQUIRED_INT64_TIME_NOT_UTC_MICROS_TYPE,
            UnsupportedTypes.REPEATED_BINARY_STRING_TYPE,
            UnsupportedTypes.OPTIONAL_MAP_FLOAT_OPTIONAL_INT32_TYPE, UnsupportedTypes.REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE,
            UnsupportedTypes.OPTIONAL_MAP_FLOAT_REQUIRED_INT32_TYPE, UnsupportedTypes.REQUIRED_MAP_FLOAT_REQUIRED_INT32_TYPE
    );

    public static final class SupportedTypes {
        /**
         * Fixed length byte array decimal.
         */
        public static final Type OPTIONAL_BINARY_DECIMAL_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.decimalType(16, 16))
                        .named("OPTIONAL_BINARY_DECIMAL_TYPE");

        /**
         * Fixed length byte array decimal.
         */
        public static final Type REQUIRED_BINARY_DECIMAL_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.decimalType(16, 16))
                        .named("REQUIRED_BINARY_DECIMAL_TYPE");

        /**
         * Zero or one instances of a binary subtype string value is required per row group.
         */
        public static final Type OPTIONAL_BINARY_STRING_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("OPTIONAL_BINARY_STRING_TYPE");

        /**
         * Exactly one instance of a binary subtype string value is required per row group.
         */
        public static final Type REQUIRED_BINARY_STRING_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("REQUIRED_BINARY_STRING_TYPE");

        /**
         * Fixed length byte array decimal.
         */
        public static final Type OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(32)
                        .as(LogicalTypeAnnotation.decimalType(16, 16))
                        .named("OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE");

        /**
         * Fixed length byte array decimal.
         */
        public static final Type REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(32)
                        .as(LogicalTypeAnnotation.decimalType(16, 16))
                        .named("REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE");

        /**
         * Zero or one instances of a boolean value is required per row group.
         */
        public static final Type OPTIONAL_BOOLEAN_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .named("OPTIONAL_BOOLEAN_TYPE");

        /**
         * Exactly one instance of a boolean value is required per row group.
         */
        public static final Type REQUIRED_BOOLEAN_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .named("REQUIRED_BOOLEAN_TYPE");

        /**
         * Zero or one instances of a double value is required per row group.
         */
        public static final Type OPTIONAL_DOUBLE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named("OPTIONAL_DOUBLE_TYPE");

        /**
         * Exactly one instance of a double value is required per row group.
         */
        public static final Type REQUIRED_DOUBLE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named("REQUIRED_DOUBLE_TYPE");

        /**
         * Zero or one instances of a float value is required per row group.
         */
        public static final Type OPTIONAL_FLOAT_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .named("OPTIONAL_FLOAT_TYPE");

        /**
         * Exactly one instance of a float value is required per row group.
         */
        public static final Type REQUIRED_FLOAT_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .named("REQUIRED_FLOAT_TYPE");

        /**
         * Zero or one instances of a signed int/int32 value is required per row group.
         */
        public static final Type OPTIONAL_INT32_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("OPTIONAL_INT32_TYPE");

        /**
         * Zero or one instances of a signed int/int32 value is required per row group.
         */
        public static final Type OPTIONAL_INT32_INT_32_TRUE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(32, true))
                        .named("OPTIONAL_INT32_INT_32_TRUE_TYPE");

        /**
         * Exactly one instance of an int/int32 value is required per row group.
         */
        public static final Type REQUIRED_INT32_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("REQUIRED_INT32_TYPE");

        /**
         * Exactly one instance of an int/int32 value is required per row group.
         */
        public static final Type REQUIRED_INT32_INT_32_TRUE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(32, true))
                        .named("REQUIRED_INT32_INT_32_TRUE_TYPE");

        public static final Type OPTIONAL_INT32_INT_16_TRUE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .named("OPTIONAL_INT32_INT_16_TRUE_TYPE");

        public static final Type REQUIRED_INT32_INT_16_TRUE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .named("REQUIRED_INT32_INT_16_TRUE_TYPE");

        public static final Type OPTIONAL_INT32_DATE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.dateType())
                        .named("OPTIONAL_INT32_DATE_TYPE");

        public static final Type REQUIRED_INT32_DATE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.dateType())
                        .named("REQUIRED_INT32_DATE_TYPE");

        public static final Type OPTIONAL_INT32_DECIMAL_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.decimalType(4, 4))
                        .named("OPTIONAL_INT32_DECIMAL_TYPE");

        public static final Type REQUIRED_INT32_DECIMAL_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.decimalType(4, 4))
                        .named("REQUIRED_INT32_DECIMAL_TYPE");

        /**
         * Zero or one instances of a long/int64 value is required per row group.
         */
        public static final Type OPTIONAL_INT64_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .named("OPTIONAL_INT64_TYPE");

        /**
         * Zero or one instances of a long/int64 value is required per row group.
         */
        public static final Type OPTIONAL_INT64_INT_64_TRUE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.intType(64, true))
                        .named("OPTIONAL_INT64_INT_64_TRUE_TYPE");

        /**
         * Exactly one instance of a long/int64 value is required per row group.
         */
        public static final Type REQUIRED_INT64_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .named("REQUIRED_INT64_TYPE");

        /**
         * Exactly one instance of a long/int64 value is required per row group.
         */
        public static final Type REQUIRED_INT64_INT_64_TRUE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.intType(64, true))
                        .named("REQUIRED_INT64_INT_64_TRUE_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE");

        public static final Type OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE");

        public static final Type REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE");

        public static final Type OPTIONAL_INT64_DECIMAL_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.decimalType(18, 18))
                        .named("OPTIONAL_INT64_DECIMAL_TYPE");

        public static final Type REQUIRED_INT64_DECIMAL_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.decimalType(18, 18))
                        .named("REQUIRED_INT64_DECIMAL_TYPE");

        private SupportedTypes() {
        }
    }

    public static final class UnsupportedTypes {
        public static final Type OPTIONAL_BINARY_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .named("OPTIONAL_BINARY_TYPE");

        public static final Type REQUIRED_BINARY_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .named("REQUIRED_BINARY_TYPE");

        /**
         * Optional fixed length byte array.
         */
        public static final Type OPTIONAL_FIXED_LEN_BYTE_ARRAY_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(5)
                        .named("OPTIONAL_FIXED_LEN_BYTE_ARRAY_TYPE");

        /**
         * Required fixed length byte array.
         */
        public static final Type REQUIRED_FIXED_LEN_BYTE_ARRAY_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(5)
                        .named("REQUIRED_FIXED_LEN_BYTE_ARRAY_TYPE");

        public static final Type OPTIONAL_BINARY_ENUM_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.enumType())
                        .named("OPTIONAL_BINARY_ENUM_TYPE");

        public static final Type REQUIRED_BINARY_ENUM_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.enumType())
                        .named("REQUIRED_BINARY_ENUM_TYPE");

        public static final Type OPTIONAL_FIXED_LEN_BYTE_ARRAY_UUID_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.uuidType())
                        .length(16)
                        .named("OPTIONAL_BINARY_UUID_TYPE");

        public static final Type REQUIRED_FIXED_LEN_BYTE_ARRAY_UUID_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.uuidType())
                        .length(16)
                        .named("REQUIRED_BINARY_UUID_TYPE");

        public static final Type OPTIONAL_BINARY_JSON_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.jsonType())
                        .named("OPTIONAL_BINARY_JSON_TYPE");

        public static final Type REQUIRED_BINARY_JSON_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.jsonType())
                        .named("REQUIRED_BINARY_JSON_TYPE");

        public static final Type OPTIONAL_BINARY_BSON_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.bsonType())
                        .named("OPTIONAL_BINARY_BSON_TYPE");

        public static final Type REQUIRED_BINARY_BSON_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.bsonType())
                        .named("REQUIRED_BINARY_BSON_TYPE");

        public static final Type OPTIONAL_INT32_INT_8_TRUE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .named("OPTIONAL_INT32_INT_8_TRUE_TYPE");

        public static final Type REQUIRED_INT32_INT_8_TRUE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .named("REQUIRED_INT32_INT_8_TRUE_TYPE");

        public static final Type OPTIONAL_INT32_INT_8_FALSE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(8, false))
                        .named("OPTIONAL_INT32_INT_8_FALSE_TYPE");

        public static final Type REQUIRED_INT32_INT_8_FALSE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(8, false))
                        .named("REQUIRED_INT32_INT_8_FALSE_TYPE");

        public static final Type OPTIONAL_INT32_INT_16_FALSE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(16, false))
                        .named("OPTIONAL_INT32_INT_16_FALSE_TYPE");

        public static final Type REQUIRED_INT32_INT_16_FALSE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(16, false))
                        .named("REQUIRED_INT32_INT_16_FALSE_TYPE");

        public static final Type OPTIONAL_INT32_INT_32_FALSE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(32, false))
                        .named("OPTIONAL_INT32_INT_32_FALSE_TYPE");

        public static final Type REQUIRED_INT32_INT_32_FALSE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(32, false))
                        .named("REQUIRED_INT32_INT_32_FALSE_TYPE");

        public static final Type OPTIONAL_INT64_INT_64_FALSE_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.intType(64, false))
                        .named("OPTIONAL_INT64_INT_64_FALSE_TYPE");

        public static final Type REQUIRED_INT64_INT_64_FALSE_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.intType(64, false))
                        .named("REQUIRED_INT64_INT_64_FALSE_TYPE");

        public static final Type OPTIONAL_INT64_TIME_UTC_NANOS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("OPTIONAL_INT64_TIME_UTC_NANOS_TYPE");

        public static final Type REQUIRED_INT64_TIME_UTC_NANOS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("REQUIRED_INT64_TIME_UTC_NANOS_TYPE");

        public static final Type OPTIONAL_INT64_TIME_NOT_UTC_NANOS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("OPTIONAL_INT64_TIME_NOT_UTC_NANOS_TYPE");

        public static final Type REQUIRED_INT64_TIME_NOT_UTC_NANOS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("REQUIRED_INT64_TIME_NOT_UTC_NANOS_TYPE");

        public static final Type OPTIONAL_INT32_TIME_UTC_MILLIS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("OPTIONAL_INT32_TIME_UTC_MILLIS_TYPE");

        public static final Type REQUIRED_INT32_TIME_UTC_MILLIS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("REQUIRED_INT32_TIME_UTC_MILLIS_TYPE");

        public static final Type OPTIONAL_INT32_TIME_NOT_UTC_MILLIS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("OPTIONAL_INT32_TIME_NOT_UTC_MILLIS_TYPE");

        public static final Type REQUIRED_INT32_TIME_NOT_UTC_MILLIS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("REQUIRED_INT32_TIME_NOT_UTC_MILLIS_TYPE");

        public static final Type OPTIONAL_INT64_TIME_UTC_MICROS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("OPTIONAL_INT64_TIME_UTC_MICRO_TYPE");

        public static final Type REQUIRED_INT64_TIME_UTC_MICROS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("REQUIRED_INT64_TIME_UTC_MICROS_TYPE");

        public static final Type OPTIONAL_INT64_TIME_NOT_UTC_MICROS_TYPE =
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("OPTIONAL_INT64_TIME_NOT_UTC_MICROS_TYPE");

        public static final Type REQUIRED_INT64_TIME_NOT_UTC_MICROS_TYPE =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("REQUIRED_INT64_TIME_NOT_UTC_MICROS_TYPE");

        /**
         * Unsupported primitive type (repeated: 0, 1 or more instances required per row group) for testing purposes.
         */
        public static final Type REPEATED_BINARY_STRING_TYPE =
                Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("REPEATED_BINARY_STRING_TYPE");

        /**
         * Unsupported logical type for testing purposes.
         */
        public static final Type OPTIONAL_MAP_FLOAT_OPTIONAL_INT32_TYPE =
                Types.optionalMap()
                        .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .optionalValue(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("OPTIONAL_MAP_FLOAT_OPTIONAL_INT32_TYPE");

        /**
         * Unsupported logical type for testing purposes.
         */
        public static final Type REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE =
                Types.requiredMap()
                        .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .optionalValue(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE");

        /**
         * Unsupported logical type for testing purposes.
         */
        public static final Type OPTIONAL_MAP_FLOAT_REQUIRED_INT32_TYPE =
                Types.optionalMap()
                        .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .requiredValue(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("OPTIONAL_MAP_FLOAT_REQUIRED_INT32_TYPE");

        /**
         * Unsupported logical type for testing purposes.
         */
        public static final Type REQUIRED_MAP_FLOAT_REQUIRED_INT32_TYPE =
                Types.requiredMap()
                        .key(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .requiredValue(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("REQUIRED_MAP_FLOAT_REQUIRED_INT32_TYPE");

        private UnsupportedTypes() {
        }
    }

    /**
     * Hidden utility constructor.
     */
    private ParquetTypeDefsTestUtility() {
    }

    public static Stream<Arguments> expectedParquetDataType() {
        return Stream.concat(
                Stream.of(
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.OPTIONAL_BINARY_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.REQUIRED_BINARY_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.STRING, SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                        Arguments.of(ClientDataType.STRING, SupportedTypes.REQUIRED_BINARY_STRING_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.BOOLEAN, SupportedTypes.OPTIONAL_BOOLEAN_TYPE),
                        Arguments.of(ClientDataType.BOOLEAN, SupportedTypes.REQUIRED_BOOLEAN_TYPE),
                        Arguments.of(ClientDataType.DOUBLE, SupportedTypes.OPTIONAL_DOUBLE_TYPE),
                        Arguments.of(ClientDataType.DOUBLE, SupportedTypes.REQUIRED_DOUBLE_TYPE),
                        Arguments.of(ClientDataType.FLOAT, SupportedTypes.OPTIONAL_FLOAT_TYPE),
                        Arguments.of(ClientDataType.FLOAT, SupportedTypes.REQUIRED_FLOAT_TYPE),
                        Arguments.of(ClientDataType.INT, SupportedTypes.OPTIONAL_INT32_TYPE),
                        Arguments.of(ClientDataType.INT, SupportedTypes.OPTIONAL_INT32_INT_32_TRUE_TYPE),
                        Arguments.of(ClientDataType.INT, SupportedTypes.REQUIRED_INT32_TYPE),
                        Arguments.of(ClientDataType.INT, SupportedTypes.REQUIRED_INT32_INT_32_TRUE_TYPE),
                        Arguments.of(ClientDataType.SMALLINT, SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE),
                        Arguments.of(ClientDataType.SMALLINT, SupportedTypes.REQUIRED_INT32_INT_16_TRUE_TYPE),
                        Arguments.of(ClientDataType.DATE, SupportedTypes.OPTIONAL_INT32_DATE_TYPE),
                        Arguments.of(ClientDataType.DATE, SupportedTypes.REQUIRED_INT32_DATE_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.OPTIONAL_INT32_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.REQUIRED_INT32_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.BIGINT, SupportedTypes.OPTIONAL_INT64_TYPE),
                        Arguments.of(ClientDataType.BIGINT, SupportedTypes.OPTIONAL_INT64_INT_64_TRUE_TYPE),
                        Arguments.of(ClientDataType.BIGINT, SupportedTypes.REQUIRED_INT64_TYPE),
                        Arguments.of(ClientDataType.BIGINT, SupportedTypes.REQUIRED_INT64_INT_64_TRUE_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE),
                        Arguments.of(ClientDataType.TIMESTAMP, SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.OPTIONAL_INT64_DECIMAL_TYPE),
                        Arguments.of(ClientDataType.DECIMAL, SupportedTypes.REQUIRED_INT64_DECIMAL_TYPE)
                ),
                ALL_UNSUPPORTED_TYPES.stream().map(type -> Arguments.of(ClientDataType.UNKNOWN, type))
        );
    }
}
