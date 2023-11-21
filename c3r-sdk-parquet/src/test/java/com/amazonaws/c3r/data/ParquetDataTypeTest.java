// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REPEATED_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_BINARY_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_INT32_INT_16_FALSE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_INT32_INT_32_FALSE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_INT64_INT_64_FALSE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetDataTypeTest {
    @Test
    public void isSupportedTypeTest() {
        for (var primitiveType : PrimitiveType.PrimitiveTypeName.values()) {
            if (primitiveType == PrimitiveType.PrimitiveTypeName.INT96) {
                assertFalse(ParquetDataType.isSupportedType(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT96).named("INT96")));
            } else if (primitiveType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                // This is its own case because the length parameter is needed.
                assertFalse(ParquetDataType.isSupportedType(REQUIRED_FIXED_LEN_BYTE_ARRAY_TYPE), "isSupportedType " + primitiveType);
            } else if (primitiveType == PrimitiveType.PrimitiveTypeName.BINARY) {
                // Binary needs a string or decimal annotation to be supported
                assertTrue(ParquetDataType.isSupportedType(REQUIRED_BINARY_DECIMAL_TYPE));
            } else {
                assertTrue(ParquetDataType.isSupportedType(Types.required(primitiveType).named(primitiveType.toString())),
                        "isSupportedType " + primitiveType);
            }
        }
        assertFalse(ParquetDataType.isSupportedType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isSupportedType(REPEATED_BINARY_STRING_TYPE));
    }

    @Test
    public void isStringTypeTest() {
        assertFalse(ParquetDataType.isStringType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isStringType(REQUIRED_INT64_TYPE));
        assertFalse(ParquetDataType.isStringType(REQUIRED_BINARY_TYPE));
        assertTrue(ParquetDataType.isStringType(REQUIRED_BINARY_STRING_TYPE));
        assertTrue(ParquetDataType.isStringType(OPTIONAL_BINARY_STRING_TYPE));
        assertTrue(ParquetDataType.isStringType(REPEATED_BINARY_STRING_TYPE));
    }

    @Test
    public void isBigIntTypeTest() {
        assertFalse(ParquetDataType.isBigIntType(ParquetTypeDefsTestUtility.UnsupportedTypes.REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isBigIntType(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isBigIntType(REQUIRED_INT64_TYPE));
        assertTrue(ParquetDataType.isBigIntType(OPTIONAL_INT64_TYPE));
        assertTrue(ParquetDataType.isBigIntType(OPTIONAL_INT64_INT_64_TRUE_TYPE));
    }

    @Test
    public void isBooleanTypeTest() {
        assertFalse(ParquetDataType.isBooleanType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isBooleanType(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isBooleanType(REQUIRED_BOOLEAN_TYPE));
        assertTrue(ParquetDataType.isBooleanType(OPTIONAL_BOOLEAN_TYPE));
    }

    @Test
    public void isDateTypeTest() {
        assertFalse(ParquetDataType.isDateType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isDateType(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isDateType(REQUIRED_INT32_DATE_TYPE));
        assertTrue(ParquetDataType.isDateType(OPTIONAL_INT32_DATE_TYPE));
    }

    @Test
    public void isDecimalTypeTest() {
        assertFalse(ParquetDataType.isDecimalType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isDecimalType(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isDecimalType(REQUIRED_INT32_DECIMAL_TYPE));
        assertTrue(ParquetDataType.isDecimalType(OPTIONAL_INT64_DECIMAL_TYPE));
        assertTrue(ParquetDataType.isDecimalType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE));
        assertTrue(ParquetDataType.isDecimalType(OPTIONAL_BINARY_DECIMAL_TYPE));
    }

    @Test
    public void isDoubleTypeTest() {
        assertFalse(ParquetDataType.isDoubleType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isDoubleType(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isDoubleType(REQUIRED_DOUBLE_TYPE));
        assertTrue(ParquetDataType.isDoubleType(OPTIONAL_DOUBLE_TYPE));
    }

    @Test
    public void isFloatTypeTest() {
        assertFalse(ParquetDataType.isFloatType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isFloatType(REQUIRED_DOUBLE_TYPE));
        assertTrue(ParquetDataType.isFloatType(REQUIRED_FLOAT_TYPE));
        assertTrue(ParquetDataType.isFloatType(OPTIONAL_FLOAT_TYPE));
    }

    @Test
    public void isInt32TypeTest() {
        assertFalse(ParquetDataType.isInt32Type(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));
        assertFalse(ParquetDataType.isInt32Type(REQUIRED_INT64_TYPE));
        assertTrue(ParquetDataType.isInt32Type(REQUIRED_INT32_TYPE));
        assertTrue(ParquetDataType.isInt32Type(REQUIRED_INT32_INT_32_TRUE_TYPE));
        assertTrue(ParquetDataType.isInt32Type(OPTIONAL_INT32_TYPE));
    }

    @Test
    public void fromTypeTest() {
        assertEquals(
                ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
                ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE));
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                ParquetDataType.fromType(REQUIRED_INT32_TYPE));
        assertEquals(ParquetDataType.fromType(REQUIRED_INT64_INT_64_TRUE_TYPE),
                ParquetDataType.fromType(REQUIRED_INT64_INT_64_TRUE_TYPE));
        assertEquals(ParquetDataType.fromType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE),
                ParquetDataType.fromType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE));

        final var parquetInt32 = ParquetDataType.fromType(REQUIRED_INT32_TYPE);
        assertNotEquals(ClientDataType.STRING, parquetInt32.getClientDataType());

        assertEquals(ClientDataType.UNKNOWN, ParquetDataType.fromType(REQUIRED_BINARY_TYPE).getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, ParquetDataType.fromType(REQUIRED_MAP_FLOAT_OPTIONAL_INT32_TYPE).getClientDataType());
        assertEquals(ClientDataType.STRING, ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE).getClientDataType());
        assertEquals(ClientDataType.STRING, ParquetDataType.fromType(OPTIONAL_BINARY_STRING_TYPE).getClientDataType());
    }

    @Test
    public void toTypeWithNameTest() {
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("FOO"),
                ParquetDataType.fromType(OPTIONAL_BINARY_STRING_TYPE)
                        .toTypeWithName("FOO")
        );

        assertEquals(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("FOO"),
                ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE)
                        .toTypeWithName("FOO")
        );

        assertEquals(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(32, true))
                        .named("FOO"),
                ParquetDataType.fromType(REQUIRED_INT32_INT_32_TRUE_TYPE).toTypeWithName("FOO")
        );

        assertThrows(C3rRuntimeException.class, () -> ParquetDataType.fromType(REPEATED_BINARY_STRING_TYPE).toTypeWithName("FOO"));
    }

    @Test
    public void binaryConstructionTest() {
        // Binary values are not supported in Clean Rooms
        final ParquetValue bin = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_TYPE),
                Binary.fromString("hello"));
        assertEquals(ClientDataType.UNKNOWN, bin.getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, bin.getParquetDataType().getClientDataType());
    }

    @Test
    public void booleanConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_INT32_TYPE), true));
        assertDoesNotThrow(() -> new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), false));
    }

    @Test
    public void doubleConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 2.71828));
        assertDoesNotThrow(() -> new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), 2.71828));
    }

    @Test
    public void floatConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_INT32_TYPE), (float) 2.71828));
        assertDoesNotThrow(() -> new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), (float) 2.71828));
    }

    @Test
    public void int32ConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 27));
        assertDoesNotThrow(() -> new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 27));
        assertDoesNotThrow(() -> new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_32_TRUE_TYPE), 27));
        assertDoesNotThrow(() -> new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_32_TRUE_TYPE), 27));
    }

    @Test
    public void int64ConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT32_TYPE), (long) 271828));
        assertDoesNotThrow(() -> new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), (long) 271828));
        assertDoesNotThrow(() -> new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_INT_64_TRUE_TYPE), (long) 271828));
        assertDoesNotThrow(() -> new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_INT_64_TRUE_TYPE), (long) 271828));
    }

    @Test
    public void unsignedIntsAreNotSupportedTest() {
        final ParquetValue int16 = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_16_FALSE_TYPE), 16);
        assertEquals(ClientDataType.UNKNOWN, int16.getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, int16.getParquetDataType().getClientDataType());
        final ParquetValue int32 = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_32_FALSE_TYPE), 32);
        assertEquals(ClientDataType.UNKNOWN, int32.getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, int32.getParquetDataType().getClientDataType());
        final ParquetValue int64 = new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_INT_64_FALSE_TYPE), 64L);
        assertEquals(ClientDataType.UNKNOWN, int64.getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, int64.getParquetDataType().getClientDataType());
    }

    @Test
    public void unannotatedBinariesAreNotSupportedTest() {
        final ParquetValue bin = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_TYPE),
                Binary.fromConstantByteArray(new byte[]{1, 2}));
        assertEquals(ClientDataType.UNKNOWN, bin.getClientDataType());
        assertEquals(ClientDataType.UNKNOWN, bin.getParquetDataType().getClientDataType());
    }
}
