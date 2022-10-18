// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.COMPLEX_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REPEATED_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_BINARY_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetDataTypeTest {

    @Test
    public void isSupportedTypeTest() {
        for (var primitiveType : PrimitiveType.PrimitiveTypeName.values()) {
            if (primitiveType == PrimitiveType.PrimitiveTypeName.INT96
                    || primitiveType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                continue;
            }
            assertTrue(ParquetDataType.isSupportedType(Types.required(primitiveType).named(primitiveType.toString())),
                    "isSupportedType " + primitiveType);
        }
        assertFalse(ParquetDataType.isSupportedType(
                Types.required(PrimitiveType.PrimitiveTypeName.INT96).named("INT96")));
        assertFalse(ParquetDataType.isSupportedType(
                Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(10).named("FIXED_LEN_BYTE_ARRAY")));
        assertFalse(ParquetDataType.isSupportedType(COMPLEX_TYPE));
        assertFalse(ParquetDataType.isSupportedType(REPEATED_STRING_TYPE));
    }

    @Test
    public void isStringTypeTest() {
        assertFalse(ParquetDataType.isStringType(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("INT32")));
        assertFalse(ParquetDataType.isStringType(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("BINARY")));
        assertFalse(ParquetDataType.isStringType(COMPLEX_TYPE));
        assertTrue(ParquetDataType.isStringType(REQUIRED_STRING_TYPE));
        assertTrue(ParquetDataType.isStringType(OPTIONAL_STRING_TYPE));

        assertTrue(
                ParquetDataType.builder()
                        .parquetType(REQUIRED_STRING_TYPE)
                        .clientDataType(ClientDataType.STRING)
                        .build()
                        .isStringType());
        assertFalse(
                ParquetDataType.builder()
                        .parquetType(REQUIRED_INT32_TYPE)
                        .clientDataType(ClientDataType.UNKNOWN)
                        .build()
                        .isStringType());
    }

    @Test
    public void fromTypeTest() {
        assertEquals(
                ParquetDataType.builder().parquetType(REQUIRED_STRING_TYPE).clientDataType(ClientDataType.STRING).build(),
                ParquetDataType.fromType(REQUIRED_STRING_TYPE));
        assertEquals(
                ParquetDataType.builder().parquetType(REQUIRED_INT32_TYPE).clientDataType(ClientDataType.UNKNOWN).build(),
                ParquetDataType.fromType(REQUIRED_INT32_TYPE));
        assertThrows(C3rIllegalArgumentException.class, () ->
                ParquetDataType.fromType(COMPLEX_TYPE));
    }

    @Test
    public void toTypeWithNameTest() {
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("FOO"),
                ParquetDataType.builder()
                        .parquetType(OPTIONAL_STRING_TYPE)
                        .clientDataType(ClientDataType.STRING)
                        .build()
                        .toTypeWithName("FOO")
        );

        assertEquals(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("FOO"),
                ParquetDataType.builder()
                        .parquetType(REQUIRED_STRING_TYPE)
                        .clientDataType(ClientDataType.STRING)
                        .build()
                        .toTypeWithName("FOO")
        );

        assertThrows(C3rIllegalArgumentException.class, () ->
                ParquetDataType.builder()
                        .parquetType(REPEATED_STRING_TYPE)
                        .clientDataType(ClientDataType.STRING)
                        .build()
                        .toTypeWithName("FOO")
        );
    }

    @Test
    public void binaryConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                        Binary.fromConstantByteArray(new byte[]{0, 0})
                )
        );
        assertDoesNotThrow(() -> new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_TYPE),
                Binary.fromString("hello"))
        );
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
                new ParquetValue.Int(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 27));
        assertDoesNotThrow(() -> new ParquetValue.Int(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 27));
    }

    @Test
    public void int64ConstructionTest() {
        assertThrows(C3rRuntimeException.class, () ->
                new ParquetValue.Long(ParquetDataType.fromType(REQUIRED_INT32_TYPE), (long) 271828));
        assertDoesNotThrow(() -> new ParquetValue.Long(ParquetDataType.fromType(REQUIRED_INT64_TYPE), (long) 271828));
    }

}
