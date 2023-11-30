// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility;
import lombok.NonNull;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_DECIMAL_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE;
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
        final ParquetValue.Binary emptyBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
                Binary.fromString(empty));
        final String value = "hello";
        final ParquetValue.Binary valueBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
                Binary.fromString(value));

        assertNull(ValueConverter.String.decode(nullBinary.getEncodedBytes()));
        assertEquals(empty, ValueConverter.String.decode(emptyBinary.getEncodedBytes()));
        assertEquals(value, ValueConverter.String.decode(valueBinary.getEncodedBytes()));
    }

    @Test
    public void encodeBinaryDecimalTest() {
        assertEquals(((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getPrecision(),
                ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                        REQUIRED_BINARY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getPrecision());
        assertEquals(((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                        REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getScale(),
                ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) REQUIRED_BINARY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getScale());
        final int precision = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getPrecision();
        final int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE.getLogicalTypeAnnotation()).getScale();

        final BigDecimal wholeNumber = new BigDecimal(new BigInteger("1000000000000000"), scale,
                new MathContext(precision, RoundingMode.HALF_UP));
        final BigDecimal overlappingNumber = new BigDecimal(new BigInteger("1000999000000000"), scale, new MathContext(precision));
        final BigDecimal fractionalNumber = new BigDecimal(new BigInteger("111020000000"), scale, new MathContext(precision));

        final ParquetValue.Binary nullFlba = new ParquetValue.Binary(
                ParquetDataType.fromType(OPTIONAL_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE), null);
        final ClientValueWithMetadata.Decimal nullFlbaResults = ValueConverter.Decimal.decode(nullFlba.getEncodedBytes());
        assertNull(nullFlbaResults.getValue());
        assertEquals(precision, nullFlbaResults.getPrecision());
        assertEquals(scale, nullFlbaResults.getScale());

        final ParquetValue.Binary nullBinary = new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_BINARY_DECIMAL_TYPE), null);
        final ClientValueWithMetadata.Decimal nullBinaryResults = ValueConverter.Decimal.decode(nullBinary.getEncodedBytes());
        assertNull(nullBinaryResults.getValue());
        assertEquals(precision, nullBinaryResults.getPrecision());
        assertEquals(scale, nullBinaryResults.getScale());

        final ParquetValue.Binary wholeNumberFlba = new ParquetValue.Binary(
                ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                Binary.fromReusedByteArray(wholeNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal wholeNumberFlbaResults = ValueConverter.Decimal.decode(wholeNumberFlba.getEncodedBytes());
        assertEquals(new BigDecimal("1000.000000000000"), wholeNumberFlbaResults.getValue());
        assertEquals(precision, wholeNumberFlbaResults.getPrecision());
        assertEquals(scale, wholeNumberFlbaResults.getScale());

        final ParquetValue.Binary wholeNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE),
                Binary.fromReusedByteArray(wholeNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal wholeNumberBinaryResults = ValueConverter.Decimal.decode(wholeNumberBinary.getEncodedBytes());
        assertEquals(new BigDecimal("1000.000000000000"), wholeNumberBinaryResults.getValue());
        assertEquals(precision, wholeNumberBinaryResults.getPrecision());
        assertEquals(scale, wholeNumberBinaryResults.getScale());

        final ParquetValue.Binary overlappingNumberFlba = new ParquetValue.Binary(
                ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                Binary.fromReusedByteArray(overlappingNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal overlappingNumberFlbaResults =
                ValueConverter.Decimal.decode(overlappingNumberFlba.getEncodedBytes());
        assertEquals(new BigDecimal("1000.999000000000"), overlappingNumberFlbaResults.getValue());
        assertEquals(precision, overlappingNumberFlbaResults.getPrecision());
        assertEquals(scale, overlappingNumberFlbaResults.getScale());

        final ParquetValue.Binary overlappingNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE),
                Binary.fromReusedByteArray(overlappingNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal overlappingNumberBinaryResults =
                ValueConverter.Decimal.decode(overlappingNumberBinary.getEncodedBytes());
        assertEquals(new BigDecimal("1000.999000000000"), overlappingNumberBinaryResults.getValue());
        assertEquals(precision, overlappingNumberBinaryResults.getPrecision());
        assertEquals(scale, overlappingNumberBinaryResults.getScale());

        final ParquetValue.Binary fractionalNumberFlba =
                new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE),
                        Binary.fromReusedByteArray(fractionalNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal fractionalNumberFlbaResults =
                ValueConverter.Decimal.decode(fractionalNumberFlba.getEncodedBytes());
        assertEquals(new BigDecimal("0.111020000000"), fractionalNumberFlbaResults.getValue());
        assertEquals(precision, fractionalNumberFlbaResults.getPrecision());
        assertEquals(scale, fractionalNumberFlbaResults.getScale());

        final ParquetValue.Binary fractionalNumberBinary = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_DECIMAL_TYPE),
                Binary.fromReusedByteArray(fractionalNumber.unscaledValue().toByteArray()));
        final ClientValueWithMetadata.Decimal fractionalNumberBinaryResults =
                ValueConverter.Decimal.decode(fractionalNumberBinary.getEncodedBytes());
        assertEquals(new BigDecimal("0.111020000000"), fractionalNumberBinaryResults.getValue());
        assertEquals(precision, fractionalNumberBinaryResults.getPrecision());
        assertEquals(scale, fractionalNumberBinaryResults.getScale());
    }

    @Test
    public void encodeBooleanTest() {
        assertNull(ValueConverter.Boolean.decode(
                new ParquetValue.Boolean(ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE), null).getEncodedBytes()));
        assertTrue(ValueConverter.Boolean.decode(
                new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), true).getEncodedBytes()));
        assertFalse(ValueConverter.Boolean.decode(
                new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE), false).getEncodedBytes()));
    }

    @Test
    public void encodeDoubleTest() {
        assertNull(ValueConverter.Double.decode(
                new ParquetValue.Double(ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE), null).getEncodedBytes()));
        assertEquals(Double.MAX_VALUE, ValueConverter.Double.decode(
                new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), Double.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeFloatTest() {
        assertNull(ValueConverter.Float.decode(
                new ParquetValue.Float(ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE), null).getEncodedBytes()));
        assertEquals(Float.MAX_VALUE, ValueConverter.Float.decode(
                new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), Float.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeDateTest() {
        final LocalDate now = LocalDate.now();
        final LocalDate epoch = LocalDate.ofEpochDay(0);
        final Long days = ChronoUnit.DAYS.between(epoch, now);
        final Long reverseDays = ChronoUnit.DAYS.between(now, epoch);

        assertNull(ValueConverter.Date.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_DATE_TYPE), null).getEncodedBytes()));
        assertEquals(days.intValue(), ValueConverter.Date.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_DATE_TYPE), days.intValue()).getEncodedBytes()));
        assertEquals(reverseDays.intValue(), ValueConverter.Date.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_DATE_TYPE), reverseDays.intValue()).getEncodedBytes()));
    }

    @Test
    public void encodeIntDecimalTest() {
        final int precision = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                REQUIRED_INT32_DECIMAL_TYPE.getLogicalTypeAnnotation()).getPrecision();
        final int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                REQUIRED_INT32_DECIMAL_TYPE.getLogicalTypeAnnotation()).getScale();

        final BigDecimal wholeNumber = new BigDecimal(new BigInteger("100000000"), scale,
                new MathContext(precision, RoundingMode.HALF_UP));
        final BigDecimal overlappingNumber = new BigDecimal(new BigInteger("100111110"), scale,
                new MathContext(precision, RoundingMode.HALF_UP));
        final BigDecimal fractionalNumber = new BigDecimal(new BigInteger("01110000"), scale,
                new MathContext(precision, RoundingMode.HALF_UP));

        final ParquetValue.Int32 nullInt = new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_DECIMAL_TYPE), null);
        final ClientValueWithMetadata.Decimal nullResults = ValueConverter.Decimal.decode(nullInt.getEncodedBytes());
        assertNull(nullResults.getValue());
        assertEquals(precision, nullResults.getPrecision());
        assertEquals(scale, nullResults.getScale());

        final ParquetValue.Int32 wholeNumberInt32 = new ParquetValue.Int32(
                ParquetDataType.fromType(REQUIRED_INT32_DECIMAL_TYPE), wholeNumber.intValueExact());
        final ClientValueWithMetadata.Decimal wholeNumberResults = ValueConverter.Decimal.decode(wholeNumberInt32.getEncodedBytes());
        assertEquals(new BigDecimal("1.000"), wholeNumberResults.getValue());
        assertEquals(precision, wholeNumberResults.getPrecision());
        assertEquals(scale, wholeNumberResults.getScale());

        final ParquetValue.Int32 overlappingNumberInt32 = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_DECIMAL_TYPE),
                overlappingNumber.intValueExact());
        final ClientValueWithMetadata.Decimal overlappingNumberResults = ValueConverter.Decimal.decode(
                overlappingNumberInt32.getEncodedBytes());
        assertEquals(new BigDecimal("1.001"), overlappingNumberResults.getValue());
        assertEquals(precision, overlappingNumberResults.getPrecision());
        assertEquals(scale, overlappingNumberResults.getScale());

        final ParquetValue.Int32 fractionalNumberInt32 = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_DECIMAL_TYPE),
                fractionalNumber.intValueExact());
        final ClientValueWithMetadata.Decimal fractionalNumberResults = ValueConverter.Decimal.decode(
                fractionalNumberInt32.getEncodedBytes());
        assertEquals(new BigDecimal("0.0111"), fractionalNumberResults.getValue());
        assertEquals(precision, fractionalNumberResults.getPrecision());
        assertEquals(scale, fractionalNumberResults.getScale());
    }

    @Test
    public void encodeIntTest() {
        assertNull(ValueConverter.Int.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_TYPE), null).getEncodedBytes()));
        assertNull(ValueConverter.Int.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_32_TRUE_TYPE), null).getEncodedBytes()));
        assertEquals(Integer.MIN_VALUE, ValueConverter.Int.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), Integer.MIN_VALUE).getEncodedBytes()));
        assertEquals(Integer.MAX_VALUE, ValueConverter.Int.decode(
                new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_32_TRUE_TYPE), Integer.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeSmallIntTest() {
        assertNull(ValueConverter.SmallInt.decode(new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_16_TRUE_TYPE), null)
                .getEncodedBytes()));
        assertEquals(Short.MIN_VALUE, ValueConverter.SmallInt.decode(new ParquetValue.Int32(
                ParquetDataType.fromType(REQUIRED_INT32_INT_16_TRUE_TYPE), (int) Short.MIN_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeBigIntTest() {
        assertNull(ValueConverter.BigInt.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TYPE), null).getEncodedBytes()));
        assertNull(ValueConverter.BigInt.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_INT_64_TRUE_TYPE), null).getEncodedBytes()));
        assertEquals(Long.MIN_VALUE, ValueConverter.BigInt.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), Long.MIN_VALUE).getEncodedBytes()));
        assertEquals(Long.MAX_VALUE, ValueConverter.BigInt.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_INT_64_TRUE_TYPE), Long.MAX_VALUE).getEncodedBytes()));
    }

    @Test
    public void encodeBigIntDecimalTest() {
        final int precision = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) REQUIRED_INT64_DECIMAL_TYPE.getLogicalTypeAnnotation())
                .getPrecision();
        final int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) REQUIRED_INT64_DECIMAL_TYPE.getLogicalTypeAnnotation())
                .getScale();

        final BigDecimal wholeNumber = new BigDecimal(new BigInteger("1234567800000000000000000000"), scale,
                new MathContext(precision, RoundingMode.HALF_UP));
        final BigDecimal overlappingNumber = new BigDecimal(new BigInteger("100099900000000000000000"), scale, new MathContext(precision));
        final BigDecimal fractionalNumber = new BigDecimal(new BigInteger("11102000000000000000"), scale, new MathContext(precision));

        final ParquetValue.Int64 nullInt = new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_DECIMAL_TYPE), null);
        final ClientValueWithMetadata.Decimal nullResults = ValueConverter.Decimal.decode(nullInt.getEncodedBytes());
        assertNull(nullResults.getValue());
        assertEquals(precision, nullResults.getPrecision());
        assertEquals(scale, nullResults.getScale());

        final ParquetValue.Int64 wholeNumberInt64 = new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_DECIMAL_TYPE),
                wholeNumber.longValueExact());
        final ClientValueWithMetadata.Decimal wholeNumberResults = ValueConverter.Decimal.decode(wholeNumberInt64.getEncodedBytes());
        assertEquals(new BigDecimal("12345678.0000000000"), wholeNumberResults.getValue());
        assertEquals(precision, wholeNumberResults.getPrecision());
        assertEquals(scale, wholeNumberResults.getScale());

        final ParquetValue.Int64 overlappingNumberInt64 = new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_DECIMAL_TYPE),
                overlappingNumber.longValueExact());
        final ClientValueWithMetadata.Decimal overlappingNumberResults = ValueConverter.Decimal.decode(
                overlappingNumberInt64.getEncodedBytes());
        assertEquals(new BigDecimal("1000.9990000000"), overlappingNumberResults.getValue());
        assertEquals(precision, overlappingNumberResults.getPrecision());
        assertEquals(scale, overlappingNumberResults.getScale());

        final ParquetValue.Int64 fractionalNumberInt64 = new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_DECIMAL_TYPE),
                fractionalNumber.longValueExact());
        final ClientValueWithMetadata.Decimal fractionalNumberResults = ValueConverter.Decimal.decode(
                fractionalNumberInt64.getEncodedBytes());
        assertEquals(new BigDecimal("0.1110200000"), fractionalNumberResults.getValue());
        assertEquals(precision, fractionalNumberResults.getPrecision());
        assertEquals(scale, fractionalNumberResults.getScale());
    }

    private static Stream<Arguments> getTimeCombos() {
        return Stream.of(
                Arguments.of(false, Units.Seconds.MICROS,
                        OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE, REQUIRED_INT64_TIMESTAMP_NOT_UTC_MICROS_TYPE),
                Arguments.of(false, Units.Seconds.MILLIS,
                        OPTIONAL_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE, REQUIRED_INT64_TIMESTAMP_NOT_UTC_MILLIS_TYPE),
                Arguments.of(false, Units.Seconds.NANOS,
                        OPTIONAL_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE, REQUIRED_INT64_TIMESTAMP_NOT_UTC_NANOS_TYPE),
                Arguments.of(true, Units.Seconds.MICROS,
                        OPTIONAL_INT64_TIMESTAMP_UTC_MICROS_TYPE, REQUIRED_INT64_TIMESTAMP_UTC_MICROS_TYPE),
                Arguments.of(true, Units.Seconds.MILLIS,
                        OPTIONAL_INT64_TIMESTAMP_UTC_MILLIS_TYPE, REQUIRED_INT64_TIMESTAMP_UTC_MILLIS_TYPE),
                Arguments.of(true, Units.Seconds.NANOS,
                        OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE, REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE)
        );
    }

    @ParameterizedTest
    @MethodSource(value = "getTimeCombos")
    public void encodeTimestampTest(final boolean isUtc, @NonNull final Units.Seconds unit, @NonNull final Type optional,
                                    @NonNull final Type required) {
        final ClientValueWithMetadata.Timestamp nullValue = ValueConverter.Timestamp.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(optional), null).getEncodedBytes());
        assertNull(nullValue.getValue());
        assertEquals(isUtc, nullValue.getIsUtc());
        assertEquals(unit, nullValue.getUnit());

        Long seconds = null;
        switch (unit) {
            case MILLIS:
                seconds = ChronoUnit.MILLIS.between(LocalDateTime.now(), LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
                break;
            case MICROS:
                seconds = ChronoUnit.MICROS.between(LocalDateTime.now(), LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
                break;
            case NANOS:
                seconds = ChronoUnit.NANOS.between(LocalDateTime.now(), LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
                break;
            default:
                throw new C3rRuntimeException("Unexpected time unit.");
        }
        final ClientValueWithMetadata.Timestamp timestampInfo = ValueConverter.Timestamp.decode(
                new ParquetValue.Int64(ParquetDataType.fromType(required), seconds).getEncodedBytes());
        assertEquals(seconds, timestampInfo.getValue());
        assertEquals(isUtc, timestampInfo.getIsUtc());
        assertEquals(unit, timestampInfo.getUnit());
    }
}
