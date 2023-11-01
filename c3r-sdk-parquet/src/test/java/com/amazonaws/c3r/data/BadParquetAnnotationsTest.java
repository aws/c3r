// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BadParquetAnnotationsTest {
    @Test
    public void binaryBadAnnotationsTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.intType(32, true)).named("binary")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.dateType()).named("binary")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)).named("binary")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("binary")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("binary")), null));
    }

    @Test
    public void booleanBadAnnotationTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.stringType()).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.intType(32, true)).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.dateType()).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("boolean")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Boolean(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("boolean")), null));
    }

    @Test
    public void doubleBadAnnotationTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).as(LogicalTypeAnnotation.stringType())
                        .named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).as(LogicalTypeAnnotation.intType(32, true))
                        .named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .as(LogicalTypeAnnotation.dateType()).named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)).named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("double")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Double(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("double")), null));
    }

    @Test
    public void floatBadAnnotationTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).as(LogicalTypeAnnotation.stringType())
                        .named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.intType(32, true)).named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.dateType()).named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)).named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("float")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Float(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("float")), null));
    }

    @Test
    public void int32BadAnnotationTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int32(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.stringType())
                        .named("int32")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int32(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)).named("int32")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int32(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("int32")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int32(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("int32")), null));
    }

    @Test
    public void int64BadAnnotationTest() {
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int64(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.stringType())
                        .named("int64")), null));
        assertThrows(IllegalStateException.class, () -> new ParquetValue.Int64(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.dateType())
                        .named("int64")), null));
    }

    @Test
    public void fixedLenByteArrayBadAnnotationTest() {
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.stringType()).named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.intType(32, true)).named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.dateType()).named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named("fixed_len_byte_array")), null));
        assertThrows(IllegalArgumentException.class, () -> new ParquetValue.Binary(
                ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("int32")), null));
    }
}
