// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.amazonaws.c3r.data.ClientDataType.BIGINT_BYTE_SIZE;
import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.COMPLEX_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_FIXED_LEN_BYTE_ARRAY;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_INT16_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.OPTIONAL_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_FIXED_LEN_BYTE_ARRAY;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_INT16_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.REQUIRED_STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParquetValueTest {
    private final ParquetValue.Binary nullFixedLenByteArray = new ParquetValue.Binary(
            ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY),
            null);

    private final ParquetValue.Binary fixedLenByteArray = new ParquetValue.Binary(
            ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY),
            org.apache.parquet.io.api.Binary.fromReusedByteArray("FIX  ".getBytes(StandardCharsets.UTF_8)));

    private final ParquetValue.Binary nullBinaryValue = new ParquetValue.Binary(
            ParquetDataType.fromType(OPTIONAL_STRING_TYPE),
            null);

    private final ParquetValue.Binary binaryValue = new ParquetValue.Binary(
            ParquetDataType.fromType(REQUIRED_STRING_TYPE),
            org.apache.parquet.io.api.Binary.fromReusedByteArray("foo".getBytes(StandardCharsets.UTF_8)));

    private final ParquetValue.Boolean nullBooleanValue = new ParquetValue.Boolean(
            ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE),
            null);

    private final ParquetValue.Boolean booleanValue = new ParquetValue.Boolean(
            ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
            true);

    private final ParquetValue.Double nullDoubleValue = new ParquetValue.Double(
            ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE),
            null);

    private final ParquetValue.Double doubleValue = new ParquetValue.Double(
            ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
            3.14159);

    private final ParquetValue.Float nullFloatValue = new ParquetValue.Float(
            ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE),
            null);

    private final ParquetValue.Float floatValue = new ParquetValue.Float(
            ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
            3.14159f);

    private final ParquetValue.Int32 nullInt16Value = new ParquetValue.Int32(
            ParquetDataType.fromType(OPTIONAL_INT16_TYPE),
            null);

    private final ParquetValue.Int32 int16Value = new ParquetValue.Int32(
            ParquetDataType.fromType(REQUIRED_INT16_TYPE),
            3);

    private final ParquetValue.Int32 nullInt32Value = new ParquetValue.Int32(
            ParquetDataType.fromType(OPTIONAL_INT32_TYPE),
            null);

    private final ParquetValue.Int32 int32Value = new ParquetValue.Int32(
            ParquetDataType.fromType(REQUIRED_INT32_TYPE),
            3);

    private final ParquetValue.Int64 nullInt64Value = new ParquetValue.Int64(
            ParquetDataType.fromType(OPTIONAL_INT64_TYPE),
            null);

    private final ParquetValue.Int64 int64Value = new ParquetValue.Int64(
            ParquetDataType.fromType(REQUIRED_INT64_TYPE),
            3L);

    private void checkEqualsAndHashCode(final ParquetValue value1,
                                        final ParquetValue value2) {
        assertEquals(value1, value1);
        assertNotEquals(value1, value2);

        assertEquals(value1.hashCode(), value1.hashCode());
        assertNotEquals(value1.hashCode(), value2.hashCode());
    }

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray("ABCDE".getBytes(StandardCharsets.UTF_8))),
                        new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray("FGHIJ".getBytes(StandardCharsets.UTF_8))),
                        new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray("ABCDE".getBytes(StandardCharsets.UTF_8)))),
                Arguments.of(new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{0, 1, 2})),
                        new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{3, 4, 5})),
                        new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_STRING_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{0, 1, 2}))),
                Arguments.of(new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), true),
                        new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), false),
                        new ParquetValue.Boolean(ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE), true)),
                Arguments.of(new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), 3.14159),
                        new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), 42.0),
                        new ParquetValue.Double(ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE), 3.14159)),
                Arguments.of(new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), 3.14159f),
                        new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), 42.0f),
                        new ParquetValue.Float(ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE), 3.14159f)),
                Arguments.of(new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT16_TYPE), 3),
                        new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT16_TYPE), 42),
                        new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT16_TYPE), 3)),
                Arguments.of(new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 3),
                        new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 42),
                        new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_TYPE), 3)),
                Arguments.of(new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 3L),
                        new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 42L),
                        new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TYPE), 3L))
        );
    }

    @ParameterizedTest
    @MethodSource(value = "getParams")
    public void equalsAndHashCodeBinaryTest(final ParquetValue val1Req, final ParquetValue val2Req, final ParquetValue val1Opt) {
        // same type, different values
        assertFalse(Arrays.equals(val1Req.getBytes(), val2Req.getBytes()));
        checkEqualsAndHashCode(val1Req, val2Req);

        // different type, same values
        assertArrayEquals(val1Req.getBytes(), val1Opt.getBytes());
        checkEqualsAndHashCode(val1Req, val1Opt);
    }

    @Test
    public void getParquetDataTypeTest() {
        assertEquals(
                ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY),
                fixedLenByteArray.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                binaryValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
                booleanValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
                doubleValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
                floatValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT16_TYPE),
                int16Value.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                int32Value.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT64_TYPE),
                int64Value.getParquetDataType());
    }

    @Test
    public void byteLengthTest() {
        assertEquals(0, nullFixedLenByteArray.byteLength());
        assertEquals(0, nullBinaryValue.byteLength());
        assertEquals(3, binaryValue.byteLength());
        assertEquals(0, nullBooleanValue.byteLength());
        assertEquals(1, booleanValue.byteLength());
        assertEquals(0, nullDoubleValue.byteLength());
        assertEquals(Double.BYTES, doubleValue.byteLength());
        assertEquals(0, nullFloatValue.byteLength());
        assertEquals(Float.BYTES, floatValue.byteLength());
        assertEquals(0, nullInt16Value.byteLength());
        assertEquals(INT_BYTE_SIZE, int16Value.byteLength());
        assertEquals(0, nullInt32Value.byteLength());
        assertEquals(INT_BYTE_SIZE, int32Value.byteLength());
        assertEquals(0, nullInt64Value.byteLength());
        assertEquals(BIGINT_BYTE_SIZE, int64Value.byteLength());
    }

    @Test
    public void isNullTest() {
        assertTrue(nullFixedLenByteArray.isNull());
        assertTrue(nullBinaryValue.isNull());
        assertFalse(binaryValue.isNull());
        assertTrue(nullBooleanValue.isNull());
        assertFalse(booleanValue.isNull());
        assertTrue(nullDoubleValue.isNull());
        assertFalse(doubleValue.isNull());
        assertTrue(nullFloatValue.isNull());
        assertFalse(floatValue.isNull());
        assertTrue(nullInt16Value.isNull());
        assertFalse(int16Value.isNull());
        assertTrue(nullInt32Value.isNull());
        assertFalse(int32Value.isNull());
        assertTrue(nullInt64Value.isNull());
        assertFalse(int64Value.isNull());
    }

    @Test
    public void toStringTest() {
        assertNull(nullFixedLenByteArray.toString());
        assertEquals("FIX  ", fixedLenByteArray.toString());
        assertNull(nullBinaryValue.toString());
        assertEquals("foo", binaryValue.toString());
        assertNull(nullBooleanValue.toString());
        assertEquals("true", booleanValue.toString());
        assertNull(nullDoubleValue.toString());
        assertEquals("3.14159", doubleValue.toString());
        assertNull(nullFloatValue.toString());
        assertEquals("3.14159", floatValue.toString());
        assertNull(nullInt16Value.toString());
        assertEquals("3", int16Value.toString());
        assertNull(nullInt32Value.toString());
        assertEquals("3", int32Value.toString());
        assertNull(nullInt64Value.toString());
        assertEquals("3", int64Value.toString());
    }

    @Test
    public void fromBytesRoundTripTest() {
        final List<ParquetValue> values = List.of(
                fixedLenByteArray,
                binaryValue,
                booleanValue,
                doubleValue,
                floatValue,
                int16Value,
                int32Value,
                int64Value
        );
        for (var value : values) {
            assertEquals(value, ParquetValue.fromBytes(value.getParquetDataType(), value.getBytes()));
        }
    }

    @Test
    public void fromBytesUnsupportedTypeTest() {
        assertThrows(C3rRuntimeException.class, () -> ParquetDataType.fromType(COMPLEX_TYPE));
    }

    @Test
    public void isExpectedTypeNonPrimitiveTest() {
        final ParquetDataType complexParquetDataType = mock(ParquetDataType.class);
        when(complexParquetDataType.getParquetType()).thenReturn(COMPLEX_TYPE);

        assertFalse(ParquetValue.isExpectedType(
                null,
                complexParquetDataType));
    }

    @Test
    public void isExpectedTypeTest() {
        final ParquetDataType int32DataType = ParquetDataType.fromType(REQUIRED_INT32_TYPE);
        final ParquetDataType int64DataType = ParquetDataType.fromType(REQUIRED_INT64_TYPE);

        assertTrue(ParquetValue.isExpectedType(
                int64DataType.getParquetType().asPrimitiveType().getPrimitiveTypeName(),
                int64DataType));
        assertFalse(ParquetValue.isExpectedType(
                int32DataType.getParquetType().asPrimitiveType().getPrimitiveTypeName(),
                int64DataType));
    }
}
