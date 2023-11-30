// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.amazonaws.c3r.data.ClientDataType.BIGINT_BYTE_SIZE;
import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParquetValueTest {
    private static final ParquetValue.Binary NULL_FIXED_LEN_BYTE_ARRAY = new ParquetValue.Binary(
            ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
            null);

    private static final BigInteger FIXED_LEN_BYTE_ARRAY_INPUT = BigInteger.ONE;

    private static final ParquetValue.Binary FIXED_LEN_BYTE_ARRAY_VALUE = new ParquetValue.Binary(
            ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
            org.apache.parquet.io.api.Binary.fromReusedByteArray(FIXED_LEN_BYTE_ARRAY_INPUT.toByteArray()));

    private static final ParquetValue.Binary NULL_BINARY_VALUE = new ParquetValue.Binary(
            ParquetDataType.fromType(OPTIONAL_BINARY_STRING_TYPE),
            null);

    private static final String BINARY_INPUT = "foo";

    private static final ParquetValue.Binary BINARY_VALUE = new ParquetValue.Binary(
            ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
            org.apache.parquet.io.api.Binary.fromReusedByteArray(BINARY_INPUT.getBytes(StandardCharsets.UTF_8)));

    private static final ParquetValue.Boolean NULL_BOOLEAN_VALUE = new ParquetValue.Boolean(
            ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE),
            null);

    private static final Boolean BOOLEAN_INPUT = true;

    private static final ParquetValue.Boolean BOOLEAN_VALUE = new ParquetValue.Boolean(
            ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
            BOOLEAN_INPUT);

    private static final ParquetValue.Double NULL_DOUBLE_VALUE = new ParquetValue.Double(
            ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE),
            null);

    private static final Double DOUBLE_INPUT = 3.14159;

    private static final ParquetValue.Double DOUBLE_VALUE = new ParquetValue.Double(
            ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
            DOUBLE_INPUT);

    private static final ParquetValue.Float NULL_FLOAT_VALUE = new ParquetValue.Float(
            ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE),
            null);

    private static final Float FLOAT_INPUT = 3.14159f;

    private static final ParquetValue.Float FLOAT_VALUE = new ParquetValue.Float(
            ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
            FLOAT_INPUT);

    private static final ParquetValue.Int32 NULL_INT_16_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(OPTIONAL_INT32_INT_16_TRUE_TYPE),
            null);

    private static final Integer INT_16_INPUT = 3;

    private static final ParquetValue.Int32 INT_16_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(REQUIRED_INT32_INT_16_TRUE_TYPE),
            INT_16_INPUT);

    private static final ParquetValue.Int32 NULL_INT_32_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(OPTIONAL_INT32_TYPE),
            null);

    private static final Integer INT_32_INPUT = 3;

    private static final ParquetValue.Int32 INT_32_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(REQUIRED_INT32_TYPE),
            INT_32_INPUT);

    private static final ParquetValue.Int64 NULL_INT_64_VALUE = new ParquetValue.Int64(
            ParquetDataType.fromType(OPTIONAL_INT64_TYPE),
            null);

    private static final Long INT_64_INPUT = 3L;

    private static final ParquetValue.Int64 INT_64_VALUE = new ParquetValue.Int64(
            ParquetDataType.fromType(REQUIRED_INT64_TYPE),
            INT_64_INPUT);

    private void checkEqualsAndHashCode(final ParquetValue value1,
                                        final ParquetValue value2) {
        assertEquals(value1, value1);
        assertNotEquals(value1, value2);

        assertEquals(value1.hashCode(), value1.hashCode());
        assertNotEquals(value1.hashCode(), value2.hashCode());
    }

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(FIXED_LEN_BYTE_ARRAY_VALUE,
                        new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(BigInteger.TEN.toByteArray())),
                        new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(FIXED_LEN_BYTE_ARRAY_INPUT.toByteArray()))),
                Arguments.of(BINARY_VALUE,
                        new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{3, 4, 5})),
                        new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_BINARY_STRING_TYPE),
                                org.apache.parquet.io.api.Binary.fromReusedByteArray(BINARY_INPUT.getBytes(StandardCharsets.UTF_8)))),
                Arguments.of(BOOLEAN_VALUE,
                        new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), false),
                        new ParquetValue.Boolean(ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE), BOOLEAN_INPUT)),
                Arguments.of(DOUBLE_VALUE,
                        new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), 42.0),
                        new ParquetValue.Double(ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE), DOUBLE_INPUT)),
                Arguments.of(FLOAT_VALUE,
                        new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), 42.0f),
                        new ParquetValue.Float(ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE), FLOAT_INPUT)),
                Arguments.of(INT_16_VALUE,
                        new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_16_TRUE_TYPE), 42),
                        new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_16_TRUE_TYPE), INT_16_INPUT)),
                Arguments.of(INT_32_VALUE,
                        new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 42),
                        new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_TYPE), INT_32_INPUT)),
                Arguments.of(INT_64_VALUE,
                        new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 42L),
                        new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TYPE), INT_64_INPUT))
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

    public static Stream<Arguments> getAllTypes() {
        return ParquetTypeDefsTestUtility.expectedParquetDataType();
    }

    @ParameterizedTest
    @MethodSource(value = "getAllTypes")
    public void getParquetDataTypeTest(final ClientDataType clientDataType, final Type parquetType) {
        final ParquetDataType type = ParquetDataType.fromType(parquetType);
        assertEquals(clientDataType, type.getClientDataType());
        assertEquals(parquetType, type.getParquetType());
    }

    @Test
    public void byteLengthTest() {
        assertEquals(0, NULL_FIXED_LEN_BYTE_ARRAY.byteLength());
        assertEquals(1, FIXED_LEN_BYTE_ARRAY_VALUE.byteLength());
        assertEquals(0, NULL_BINARY_VALUE.byteLength());
        assertEquals(3, BINARY_VALUE.byteLength());
        assertEquals(0, NULL_BOOLEAN_VALUE.byteLength());
        assertEquals(1, BOOLEAN_VALUE.byteLength());
        assertEquals(0, NULL_DOUBLE_VALUE.byteLength());
        assertEquals(Double.BYTES, DOUBLE_VALUE.byteLength());
        assertEquals(0, NULL_FLOAT_VALUE.byteLength());
        assertEquals(Float.BYTES, FLOAT_VALUE.byteLength());
        assertEquals(0, NULL_INT_16_VALUE.byteLength());
        assertEquals(INT_BYTE_SIZE, INT_16_VALUE.byteLength());
        assertEquals(0, NULL_INT_32_VALUE.byteLength());
        assertEquals(INT_BYTE_SIZE, INT_32_VALUE.byteLength());
        assertEquals(0, NULL_INT_64_VALUE.byteLength());
        assertEquals(BIGINT_BYTE_SIZE, INT_64_VALUE.byteLength());
    }

    @Test
    public void isNullTest() {
        assertTrue(NULL_FIXED_LEN_BYTE_ARRAY.isNull());
        assertTrue(NULL_BINARY_VALUE.isNull());
        assertFalse(BINARY_VALUE.isNull());
        assertTrue(NULL_BOOLEAN_VALUE.isNull());
        assertFalse(BOOLEAN_VALUE.isNull());
        assertTrue(NULL_DOUBLE_VALUE.isNull());
        assertFalse(DOUBLE_VALUE.isNull());
        assertTrue(NULL_FLOAT_VALUE.isNull());
        assertFalse(FLOAT_VALUE.isNull());
        assertTrue(NULL_INT_16_VALUE.isNull());
        assertFalse(INT_16_VALUE.isNull());
        assertTrue(NULL_INT_32_VALUE.isNull());
        assertFalse(INT_32_VALUE.isNull());
        assertTrue(NULL_INT_64_VALUE.isNull());
        assertFalse(INT_64_VALUE.isNull());
    }

    @Test
    public void toStringTest() {
        assertNull(NULL_FIXED_LEN_BYTE_ARRAY.toString());
        assertEquals("Binary{1 constant bytes, [1]}", FIXED_LEN_BYTE_ARRAY_VALUE.toString());
        assertNull(NULL_BINARY_VALUE.toString());
        assertEquals(BINARY_INPUT, BINARY_VALUE.toString());
        assertNull(NULL_BOOLEAN_VALUE.toString());
        assertEquals(BOOLEAN_INPUT.toString(), BOOLEAN_VALUE.toString());
        assertNull(NULL_DOUBLE_VALUE.toString());
        assertEquals(DOUBLE_INPUT.toString(), DOUBLE_VALUE.toString());
        assertNull(NULL_FLOAT_VALUE.toString());
        assertEquals(FLOAT_INPUT.toString(), FLOAT_VALUE.toString());
        assertNull(NULL_INT_16_VALUE.toString());
        assertEquals(INT_16_INPUT.toString(), INT_16_VALUE.toString());
        assertNull(NULL_INT_32_VALUE.toString());
        assertEquals(INT_32_INPUT.toString(), INT_32_VALUE.toString());
        assertNull(NULL_INT_64_VALUE.toString());
        assertEquals(INT_64_INPUT.toString(), INT_64_VALUE.toString());
    }

    @Test
    public void fromBytesRoundTripTest() {
        final List<ParquetValue> values = List.of(
                FIXED_LEN_BYTE_ARRAY_VALUE,
                BINARY_VALUE,
                BOOLEAN_VALUE,
                DOUBLE_VALUE,
                FLOAT_VALUE,
                INT_16_VALUE,
                INT_32_VALUE,
                INT_64_VALUE
        );
        for (var value : values) {
            assertEquals(value, ParquetValue.fromBytes(value.getParquetDataType(), value.getBytes()));
        }
    }

    @Test
    public void isExpectedTypeNonPrimitiveTest() {
        final ParquetDataType complexParquetDataType = mock(ParquetDataType.class);
        when(complexParquetDataType.getParquetType())
                .thenReturn(ParquetTypeDefsTestUtility.UnsupportedTypes.OPTIONAL_MAP_FLOAT_OPTIONAL_INT32_TYPE);

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

    @Test
    public void encodeBinaryStringTest() {
        final ParquetValue.Binary nullBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE), null);
        final String empty = "";
        final ParquetValue.Binary emptyBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE), Binary.fromString(empty));
        final String value = "hello";
        final ParquetValue.Binary valueBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE), Binary.fromString(value));

        assertNull(ValueConverter.String.decode(nullBinary.getEncodedBytes()));
        assertEquals(empty, ValueConverter.String.decode(emptyBinary.getEncodedBytes()));
        assertEquals(value, ValueConverter.String.decode(valueBinary.getEncodedBytes()));
    }

    @Test
    public void encodeBinaryDecimalTest() {
        final ParquetValue.Binary nullFLBA = new ParquetValue.Binary(
                ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE), null);
        final ParquetValue.Binary nullBinary = new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_BINARY_DECIMAL_TYPE), null);
        final int precision = 20;
        final int scale = 12;
        final BigDecimal wholeNumber = new BigDecimal("1000", new MathContext(precision)).setScale(scale);
        final ParquetValue.Binary wholeNumberFLBA = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE), Binary.fromReusedByteArray(wholeNumber.unscaledValue().toByteArray()));
        final ParquetValue.Binary wholeNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE), Binary.fromReusedByteArray(wholeNumber.unscaledValue().toByteArray()));
        final BigDecimal overlappingNumber = new BigDecimal("1000.999", new MathContext(precision)).setScale(scale);
        final ParquetValue.Binary overlappingNumberFLBA = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE), Binary.fromReusedByteArray(overlappingNumber.unscaledValue().toByteArray()));
        final ParquetValue.Binary overlappingNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE), Binary.fromReusedByteArray(overlappingNumber.unscaledValue().toByteArray()));
        final BigDecimal fractionalNumber = new BigDecimal("0.11102", new MathContext(precision)).setScale(scale);
        final ParquetValue.Binary fractionalNumberFLBA = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE), Binary.fromReusedByteArray(fractionalNumber.unscaledValue().toByteArray()));
        final ParquetValue.Binary fractionalNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE), Binary.fromReusedByteArray(fractionalNumber.unscaledValue().toByteArray()));

        final ClientValueWithMetadata.Decimal nullFLBAResults = ValueConverter.Decimal.decode(nullFLBA.getEncodedBytes());
        assertNull(nullFLBAResults.getValue());
        assertEquals(precision, nullFLBAResults.getPrecision());
        assertEquals(scale, nullFLBAResults.getScale());
        final ClientValueWithMetadata.Decimal nullBinaryResults = ValueConverter.Decimal.decode(nullBinary.getEncodedBytes());
        assertNull(nullBinaryResults.getValue());
        assertEquals(precision, nullBinaryResults.getPrecision());
        assertEquals(scale, nullBinaryResults.getScale());
        final ClientValueWithMetadata.Decimal wholeNumberFLBAResults = ValueConverter.Decimal.decode(wholeNumberFLBA.getEncodedBytes());
        assertEquals(new BigDecimal("1000.000000000000"), wholeNumberFLBAResults.getValue());
        final ClientValueWithMetadata.Decimal wholeNumberBinaryResults = ValueConverter.Decimal.decode(wholeNumberBinary.getEncodedBytes());
        final ClientValueWithMetadata.Decimal overlappingNumberFLBAResults = ValueConverter.Decimal.decode(overlappingNumberFLBA.getEncodedBytes());
        assertEquals(new BigDecimal("1000.999000000000"), overlappingNumberFLBAResults.getValue());
        final ClientValueWithMetadata.Decimal overlappingNumberBinaryResults = ValueConverter.Decimal.decode(overlappingNumberBinary.getEncodedBytes());
        final ClientValueWithMetadata.Decimal fractionalNumberFLBAResults = ValueConverter.Decimal.decode(fractionalNumberFLBA.getEncodedBytes());
        assertEquals(new BigDecimal("0.111020000000"), fractionalNumberFLBAResults.getValue());
        final ClientValueWithMetadata.Decimal fractionalNumberBinaryResults = ValueConverter.Decimal.decode(fractionalNumberBinary.getEncodedBytes());
    }

    @Test
    public void encodeBooleanTest() {
        assertNull(ValueConverter.Boolean.decode(new ParquetValue.Boolean(ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE), null).getEncodedBytes()));
        assertTrue(ValueConverter.Boolean.decode(new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), true).getEncodedBytes()));
        assertFalse(ValueConverter.Boolean.decode(new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), false).getEncodedBytes()));
    }

    @Test
    public void encodeDoubleTest() {
        assertNull(ValueConverter.Double.decode(new ParquetValue.Double(ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE), null).getEncodedBytes()));
        assertEquals(Double.MAX_VALUE, ValueConverter.Double.decode(new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), Double.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeFloatTest() {
        assertNull(ValueConverter.Float.decode(new ParquetValue.Float(ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE), null).getEncodedBytes()));
        assertEquals(Float.MAX_VALUE, ValueConverter.Float.decode(new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), Float.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeDateTest() {
    }

    @Test
    public void encodeIntDecimalTest() {
    }

    @Test
    public void encodeIntTest() {
    }

    @Test
    public void encodeSmallIntTest() {
    }

    @Test
    public void encodeBigIntTest() {
    }

    @Test
    public void encodeBigIntDecimalTest() {
    }

    @Test
    public void encodeTimestampTest() {
    }
}
