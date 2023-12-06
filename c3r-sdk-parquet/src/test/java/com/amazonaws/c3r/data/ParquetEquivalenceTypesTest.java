// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.action.ParquetRowMarshaller;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.ParquetConfig;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.ParquetRowWriter;
import com.amazonaws.c3r.utils.FileTestUtility;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.amazonaws.c3r.data.ClientDataType.BIGINT_BYTE_SIZE;
import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;
import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static com.amazonaws.c3r.utils.ParquetTestUtility.readAllRows;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BINARY_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_DATE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_16_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_INT_32_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_INT_64_TRUE_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE;
import static com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility.SupportedTypes.REQUIRED_INT64_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetEquivalenceTypesTest {
    private static final ClientSettings PRESERVE_NULLS_SETTINGS = ClientSettings.lowAssuranceMode();

    private static final ColumnInsight EMPTY_COLUMN_INSIGHT = new ColumnInsight(ColumnSchema.builder().type(ColumnType.FINGERPRINT)
            .sourceHeader(ColumnHeader.ofRaw("Empty")).build(), PRESERVE_NULLS_SETTINGS);

    private static final ParquetValue.Binary NULL_STRING_VALUE =
            new ParquetValue.Binary(ParquetDataType.fromType(OPTIONAL_BINARY_STRING_TYPE), null);

    private static final ParquetValue.Binary STRING_VALUE = new ParquetValue.Binary(ParquetDataType.fromType(REQUIRED_BINARY_STRING_TYPE),
            org.apache.parquet.io.api.Binary.fromReusedByteArray("hello".getBytes(StandardCharsets.UTF_8)));

    private static final ParquetValue.Boolean NULL_BOOLEAN_VALUE = new ParquetValue.Boolean(ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE),
            null);

    private static final ParquetValue.Boolean BOOLEAN_VALUE = new ParquetValue.Boolean(ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
            true);

    private static final ParquetValue.Double NULL_DOUBLE_VALUE = new ParquetValue.Double(ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE),
            null);

    private static final ParquetValue.Double DOUBLE_VALUE = new ParquetValue.Double(ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE), 1.1);

    private static final ParquetValue.Float NULL_FLOAT_VALUE = new ParquetValue.Float(ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE), null);

    private static final ParquetValue.Float FLOAT_VALUE = new ParquetValue.Float(ParquetDataType.fromType(REQUIRED_FLOAT_TYPE), 1.1F);

    private static final ParquetValue.Int32 NULL_INT_32_VALUE = new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_TYPE), null);

    private static final ParquetValue.Int32 INT_32_VALUE = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_TYPE), 1);

    private static final ParquetValue.Int32 NULL_ANNOTATED_INT_32_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(OPTIONAL_INT32_INT_32_TRUE_TYPE), null);

    private static final ParquetValue.Int32 ANNOTATED_INT_32_VALUE = new ParquetValue.Int32(
            ParquetDataType.fromType(REQUIRED_INT32_INT_32_TRUE_TYPE), 1);

    private static final ParquetValue.Int64 NULL_INT_64_VALUE = new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TYPE), null);

    private static final ParquetValue.Int64 INT_64_VALUE = new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TYPE), 1L);

    private static final ParquetValue.Int64 NULL_ANNOTATED_INT_64_VALUE = new ParquetValue.Int64(
            ParquetDataType.fromType(OPTIONAL_INT64_INT_64_TRUE_TYPE), null);

    private static final ParquetValue.Int64 ANNOTATED_INT_64_VALUE = new ParquetValue.Int64(
            ParquetDataType.fromType(REQUIRED_INT64_INT_64_TRUE_TYPE), 1L);

    private static final ParquetValue.Int32 NULL_DATE_VALUE =
            new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_DATE_TYPE), null);

    private static final ParquetValue.Int32 DATE_VALUE = new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_DATE_TYPE), 1);

    private static final ParquetValue.Int32 NULL_INT_16_VALUE =
            new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_16_TRUE_TYPE), null);

    private static final ParquetValue.Int32 INT_16_VALUE =
            new ParquetValue.Int32(ParquetDataType.fromType(REQUIRED_INT32_INT_16_TRUE_TYPE), 1);

    private static final ParquetValue.Int64 NULL_UTC_TIMESTAMP_VALUE =
            new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE), null);

    private static final ParquetValue.Int64 UTC_TIMESTAMP_VALUE =
            new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE), 1L);

    private static final ParquetValue.Int64 NULL_TIMESTAMP_VALUE =
            new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TIMESTAMP_UTC_NANOS_TYPE), null);

    private static final ParquetValue.Int64 TIMESTAMP_VALUE =
            new ParquetValue.Int64(ParquetDataType.fromType(REQUIRED_INT64_TIMESTAMP_UTC_NANOS_TYPE), 1L);

    private static final Function<byte[], String> LONG_BYTES_TO_STRING = (val) -> String.valueOf(ByteBuffer.wrap(val).getLong());

    private static final Function<byte[], String> STRING_BYTES_TO_STRING = (val) -> new String(val, StandardCharsets.UTF_8);

    private static final Function<byte[], String> BOOLEAN_BYTES_TO_STRING = (val) -> ValueConverter.Boolean.fromBytes(val).toString();

    private static final Function<byte[], String> DATE_BYTES_TO_STRING = (val) -> String.valueOf(ByteBuffer.wrap(val).getInt());

    private static final Function<byte[], String> TIMESTAMP_BYTES_TO_STRING = (val) -> new BigInteger(val).toString();

    private static Stream<Arguments> nullSupportedTypes() {
        return Stream.of(
                Arguments.of(NULL_BOOLEAN_VALUE),
                Arguments.of(NULL_STRING_VALUE),
                Arguments.of(NULL_INT_32_VALUE),
                Arguments.of(NULL_ANNOTATED_INT_32_VALUE),
                Arguments.of(NULL_INT_64_VALUE),
                Arguments.of(NULL_ANNOTATED_INT_64_VALUE),
                Arguments.of(NULL_INT_16_VALUE),
                Arguments.of(NULL_DATE_VALUE)
        );
    }

    private static Stream<Arguments> supportedTypes() {
        return Stream.of(
                Arguments.of(BOOLEAN_VALUE, 1, BOOLEAN_BYTES_TO_STRING),
                Arguments.of(STRING_VALUE, STRING_VALUE.byteLength(), STRING_BYTES_TO_STRING),
                Arguments.of(INT_16_VALUE, BIGINT_BYTE_SIZE, LONG_BYTES_TO_STRING),
                Arguments.of(INT_32_VALUE, BIGINT_BYTE_SIZE, LONG_BYTES_TO_STRING),
                Arguments.of(ANNOTATED_INT_32_VALUE, BIGINT_BYTE_SIZE, LONG_BYTES_TO_STRING),
                Arguments.of(INT_64_VALUE, BIGINT_BYTE_SIZE, LONG_BYTES_TO_STRING),
                Arguments.of(ANNOTATED_INT_64_VALUE, BIGINT_BYTE_SIZE, LONG_BYTES_TO_STRING),
                Arguments.of(DATE_VALUE, INT_BYTE_SIZE, DATE_BYTES_TO_STRING)
        );
    }

    private static Stream<Arguments> nullUnsupportedTypes() {
        return Stream.of(
                Arguments.of(NULL_DOUBLE_VALUE),
                Arguments.of(NULL_FLOAT_VALUE),
                Arguments.of(NULL_TIMESTAMP_VALUE),
                Arguments.of(NULL_UTC_TIMESTAMP_VALUE)
        );
    }

    private static Stream<Arguments> unsupportedTypes() {
        return Stream.of(
                Arguments.of(DOUBLE_VALUE),
                Arguments.of(FLOAT_VALUE),
                Arguments.of(TIMESTAMP_VALUE, 1, TIMESTAMP_BYTES_TO_STRING),
                Arguments.of(UTC_TIMESTAMP_VALUE, 1, TIMESTAMP_BYTES_TO_STRING)
        );
    }

    @ParameterizedTest
    @MethodSource("supportedTypes")
    public void validTypesTest(final ParquetValue value, final int length, final Function toString) {
        final byte[] bytes = ValueConverter.getBytesForColumn(value, EMPTY_COLUMN_INSIGHT.getType(), PRESERVE_NULLS_SETTINGS);
        assertEquals(length + ClientDataInfo.BYTE_LENGTH, bytes.length);
        assertEquals(value.toString(), toString.apply(Arrays.copyOfRange(bytes, 0, length)));
    }

    @ParameterizedTest
    @MethodSource("nullSupportedTypes")
    public void nullValueTest(final ParquetValue value) {
        assertNotNull(ValueConverter.getBytesForColumn(value, EMPTY_COLUMN_INSIGHT.getType(), ClientSettings.highAssuranceMode()));
        assertNull(ValueConverter.getBytesForColumn(value, EMPTY_COLUMN_INSIGHT.getType(), PRESERVE_NULLS_SETTINGS));
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void invalidTypesTest(final ParquetValue value) {
        assertThrows(C3rRuntimeException.class,
                () -> ValueConverter.getBytesForColumn(value, EMPTY_COLUMN_INSIGHT.getType(), PRESERVE_NULLS_SETTINGS));
    }

    @ParameterizedTest
    @MethodSource("nullUnsupportedTypes")
    public void nullInvalidTypesTest(final ParquetValue value) {
        assertThrows(C3rRuntimeException.class,
                () -> ValueConverter.getBytesForColumn(value, EMPTY_COLUMN_INSIGHT.getType(), ClientSettings.highAssuranceMode()));
    }

    @Test
    public void marshallerWritesSameValuesTest() throws IOException {
        final ColumnHeader int16Header = new ColumnHeader(OPTIONAL_INT32_INT_16_TRUE_TYPE.getName());
        final ColumnHeader int32Header = new ColumnHeader(OPTIONAL_INT32_TYPE.getName());
        final ColumnHeader annotatedInt32Header = new ColumnHeader(OPTIONAL_INT32_INT_32_TRUE_TYPE.getName());
        final ColumnHeader int64Header = new ColumnHeader(OPTIONAL_INT64_TYPE.getName());
        final ColumnHeader annotatedInt64Header = new ColumnHeader(OPTIONAL_INT64_INT_64_TRUE_TYPE.getName());

        final TableSchema schema = new MappedTableSchema(List.of(
                ColumnSchema.builder().type(ColumnType.FINGERPRINT).sourceHeader(int16Header).targetHeader(int16Header).build(),
                ColumnSchema.builder().type(ColumnType.FINGERPRINT).sourceHeader(int32Header).targetHeader(int32Header).build(),
                ColumnSchema.builder().type(ColumnType.FINGERPRINT).sourceHeader(annotatedInt32Header).targetHeader(annotatedInt32Header)
                        .build(),
                ColumnSchema.builder().type(ColumnType.FINGERPRINT).sourceHeader(int64Header).targetHeader(int64Header).build(),
                ColumnSchema.builder().type(ColumnType.FINGERPRINT).sourceHeader(annotatedInt64Header).targetHeader(annotatedInt64Header)
                        .build()
        ));

        final ParquetValue.Int32 int16 = new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_16_TRUE_TYPE), 27);

        final ParquetValue.Int32 int32 = new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_TYPE), 27);

        final ParquetValue.Int32 annotatedInt32 = new ParquetValue.Int32(ParquetDataType.fromType(OPTIONAL_INT32_INT_32_TRUE_TYPE), 27);

        final ParquetValue.Int64 int64 = new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_TYPE), 27L);

        final ParquetValue.Int64 annotatedInt64 = new ParquetValue.Int64(ParquetDataType.fromType(OPTIONAL_INT64_INT_64_TRUE_TYPE), 27L);

        final HashMap<ColumnHeader, ParquetDataType> rowMap = new HashMap<>();
        rowMap.put(int16Header, int16.getParquetDataType());
        rowMap.put(int32Header, int32.getParquetDataType());
        rowMap.put(annotatedInt32Header, annotatedInt32.getParquetDataType());
        rowMap.put(int64Header, int64.getParquetDataType());
        rowMap.put(annotatedInt64Header, annotatedInt64.getParquetDataType());

        final ParquetValueFactory valueFactory = new ParquetValueFactory(rowMap);
        final Row<ParquetValue> row = valueFactory.newRow();
        row.putValue(int16Header, int16);
        row.putValue(int32Header, int32);
        row.putValue(annotatedInt32Header, annotatedInt32);
        row.putValue(int64Header, int64);
        row.putValue(annotatedInt64Header, annotatedInt64);

        final ParquetSchema parquetSchema = ParquetSchema.builder().messageType(
                new MessageType("IntegralTypes", List.of(
                        OPTIONAL_INT32_INT_16_TRUE_TYPE,
                        OPTIONAL_INT32_TYPE,
                        OPTIONAL_INT32_INT_32_TRUE_TYPE,
                        OPTIONAL_INT64_TYPE,
                        OPTIONAL_INT64_INT_64_TRUE_TYPE
                ))
        ).build();

        final String tempDir = FileTestUtility.createTempDir().toString();
        final Path input = FileTestUtility.resolve("input.parquet");
        final Path output = FileTestUtility.resolve("output.parquet");

        final ParquetRowWriter writer = ParquetRowWriter.builder().targetName(input.toString()).parquetSchema(parquetSchema).build();
        writer.writeRow(row);
        writer.flush();
        writer.close();

        final ClientSettings settings =
                ClientSettings.builder()
                        .preserveNulls(false)
                        .allowDuplicates(true)
                        .allowJoinsOnColumnsWithDifferentNames(true)
                        .allowCleartext(true)
                        .build();

        final EncryptConfig config = EncryptConfig.builder()
                .sourceFile(input.toString())
                .targetFile(output.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(settings)
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final var marshaller = ParquetRowMarshaller.newInstance(config, ParquetConfig.DEFAULT);

        marshaller.marshal();
        marshaller.close();

        final Row<ParquetValue> marshalledRow = readAllRows(output.toString()).get(0);
        final String int16Result = marshalledRow.getValue(int16Header).toString();
        final String int32Result = marshalledRow.getValue(int32Header).toString();
        final String annotatedInt32Result = marshalledRow.getValue(annotatedInt32Header).toString();
        final String int64Result = marshalledRow.getValue(int64Header).toString();
        final String annotatedInt64Result = marshalledRow.getValue(annotatedInt64Header).toString();

        assertEquals(int64Result, int16Result);
        assertEquals(int64Result, int32Result);
        assertEquals(int32Result, annotatedInt32Result);
        assertEquals(int64Result, annotatedInt64Result);
    }
}
