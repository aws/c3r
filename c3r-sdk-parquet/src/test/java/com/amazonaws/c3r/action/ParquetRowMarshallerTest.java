// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.ParquetConfig;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.ParquetTestUtility;
import com.amazonaws.c3r.utils.ParquetTypeDefsTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static com.amazonaws.c3r.utils.GeneralTestUtility.cleartextColumn;
import static com.amazonaws.c3r.utils.GeneralTestUtility.fingerprintColumn;
import static com.amazonaws.c3r.utils.GeneralTestUtility.sealedColumn;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_BINARY_VALUES_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_NULL_1_ROW_PRIM_DATA_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_TEST_DATA_HEADERS;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_TEST_DATA_TYPES;
import static com.amazonaws.c3r.utils.ParquetTestUtility.readAllRows;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetRowMarshallerTest {
    // a table schema which leaves each column as-is/unencrypted
    private final TableSchema identitySchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .map(h -> cleartextColumn(h.toString()))
                    .collect(Collectors.toList()));

    // a table scheme which omits the one string column and makes the rest unencrypted
    private final TableSchema dropStringColumnSchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .filter(h -> !PARQUET_TEST_DATA_TYPES.get(h).equals(ClientDataType.STRING))
                    .map(h -> cleartextColumn(h.toString()))
                    .collect(Collectors.toList()));

    // a table schema which encrypts the string column both for sealed and fingerprint
    private final TableSchema encryptStringColumnSealedAndFingerprintSchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .flatMap(h -> PARQUET_TEST_DATA_TYPES.get(h).equals(ClientDataType.STRING)
                            ?
                            Stream.of(
                                    sealedColumn(h.toString(), h + ColumnHeader.DEFAULT_SEALED_SUFFIX, PadType.NONE, null),
                                    fingerprintColumn(h.toString(), h + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX))
                            :
                            Stream.of(cleartextColumn(h.toString())))
                    .collect(Collectors.toList()));

    private final ClientSettings lowSecurityEncryptNull =
            ClientSettings.builder()
                    .preserveNulls(false)
                    .allowDuplicates(true)
                    .allowJoinsOnColumnsWithDifferentNames(true)
                    .allowCleartext(true)
                    .build();

    private String tempDir;

    private Path output;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = FileTestUtility.createTempDir().toString();
        output = FileTestUtility.resolve("output.parquet");
    }

    @Test
    public void validateRejectNonParquetFormatTest() throws IOException {
        final String output = FileTestUtility.resolve("endToEndMarshalOut.unknown").toString();
        final var configBuilder = EncryptConfig.builder()
                .sourceFile(PARQUET_1_ROW_PRIM_DATA_PATH)
                .targetFile(output)
                .fileFormat(FileFormat.CSV)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(lowSecurityEncryptNull)
                .tableSchema(identitySchema)
                .overwrite(true);

        assertThrows(C3rIllegalArgumentException.class, () ->
                ParquetRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.CSV).build(), ParquetConfig.DEFAULT));
        assertDoesNotThrow(() ->
                ParquetRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.PARQUET).build(), ParquetConfig.DEFAULT));
    }

    private void marshal1RowTest(final TableSchema schema, final ClientSettings settings, final boolean isDataNull) throws IOException {
        final String inputFile = isDataNull
                ? PARQUET_NULL_1_ROW_PRIM_DATA_PATH
                : PARQUET_1_ROW_PRIM_DATA_PATH;
        final var config = EncryptConfig.builder()
                .sourceFile(inputFile)
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
        assertNotEquals(0, Files.size(output));

        final Row<ParquetValue> inRow = readAllRows(inputFile).get(0);
        final List<Row<ParquetValue>> marshalledRows = readAllRows(output.toString());

        // The input file had one row - ensure the output does
        assertEquals(1, marshalledRows.size());
        final Row<ParquetValue> outRow = marshalledRows.get(0);
        // The marshalled row should have the size the schema dictates
        assertEquals(schema.getColumns().size(), outRow.size());

        for (ColumnSchema column : schema.getColumns()) {
            final var inValue = inRow.getValue(column.getSourceHeader());
            final var outValue = outRow.getValue(column.getTargetHeader());

            if (column.getType() == ColumnType.CLEARTEXT) {
                // cleartext content should be unchanged
                assertEquals(inValue, outValue);
            } else if (isDataNull && settings.isPreserveNulls()) {
                // null entries should remain null when preserveNULLs is true
                assertTrue(inValue.isNull());
                assertTrue(outValue.isNull());
            } else {
                // Sealed/Fingerprint data, and either it is NULL or preserveNULLs is false
                assertNotEquals(inValue, outValue);
            }
        }
    }

    private void marshall1Row(final TableSchema schema) throws IOException {
        marshal1RowTest(schema, ClientSettings.lowAssuranceMode(), /* All NULL */ false);
        marshal1RowTest(schema, lowSecurityEncryptNull, /* All NULL */ true);
        marshal1RowTest(schema, ClientSettings.lowAssuranceMode(), /* All NULL */ false);
        marshal1RowTest(schema, lowSecurityEncryptNull, /* All NULL */ true);
    }

    @Test
    public void marshal1RowIdentitySchema1RowTest() throws IOException {
        marshall1Row(identitySchema);
    }

    @Test
    public void marshal1RowDrop1ColumnSchema1RowTest() throws IOException {
        marshall1Row(dropStringColumnSchema);
    }

    @Test
    public void marshal1RowEncrypt1ColumnSchema1NullRowTest() throws IOException {
        marshall1Row(encryptStringColumnSealedAndFingerprintSchema);
    }

    private RowMarshaller<ParquetValue> buildRowMarshallerWithSchema(final TableSchema schema) {
        final var config = EncryptConfig.builder()
                .sourceFile(PARQUET_1_ROW_PRIM_DATA_PATH)
                .targetFile(output.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(lowSecurityEncryptNull)
                .tableSchema(schema)
                .overwrite(true)
                .build();
        return ParquetRowMarshaller.newInstance(config, ParquetConfig.DEFAULT);
    }

    @Test
    public void positionalSchemaErrorsTest() {
        final TableSchema positionalSchema =
                new PositionalTableSchema(identitySchema.getColumns().stream().map(c ->
                                List.of(ColumnSchema.builder()
                                        .targetHeader(c.getTargetHeader())
                                        .type(c.getType())
                                        .pad(c.getPad())
                                        .build()))
                        .collect(Collectors.toList()));
        // check that the test case works with the mapped identity schema
        assertDoesNotThrow(() ->
                buildRowMarshallerWithSchema(identitySchema));
        // check that switching to the positional identity schema errors
        assertThrows(C3rIllegalArgumentException.class, () ->
                buildRowMarshallerWithSchema(positionalSchema));

    }

    @Test
    public void marshalBinaryValuesAsStringTest() {
        final String input = PARQUET_BINARY_VALUES_PATH;
        final ColumnHeader fingerprintHeader = new ColumnHeader("fingerprint");
        final ColumnHeader sealedHeader = new ColumnHeader("sealed");
        final ColumnHeader cleartextHeader = new ColumnHeader("cleartext");

        // Output one column of each type
        final MappedTableSchema schema = new MappedTableSchema(List.of(
                ColumnSchema.builder().type(ColumnType.FINGERPRINT)
                        .sourceHeader(fingerprintHeader).targetHeader(fingerprintHeader).build(),
                ColumnSchema.builder().type(ColumnType.SEALED)
                        .sourceHeader(sealedHeader).targetHeader(sealedHeader)
                        .pad(Pad.DEFAULT).build(),
                ColumnSchema.builder().type(ColumnType.CLEARTEXT)
                        .sourceHeader(cleartextHeader).targetHeader(cleartextHeader)
                        .build()
        ));

        // All configuration settings except for how to treat binary values
        final var baseConfig = EncryptConfig.builder()
                .sourceFile(input)
                .targetFile(output.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(lowSecurityEncryptNull)
                .tableSchema(schema)
                .overwrite(true)
                .build();

        // --parquetBinaryAsString is unset should cause binaries to be treated as an unsupported value which can't be marshalled
        final var nullConfig = ParquetConfig.builder().binaryAsString(null).build();
        final RowMarshaller<ParquetValue> nullMarshaller = ParquetRowMarshaller.newInstance(baseConfig, nullConfig);
        assertThrows(C3rRuntimeException.class, () -> nullMarshaller.marshal());

        // --parquetBinaryAsString is false should cause binaries to be treated as an unsupported value which can't be marshalled
        final var falseConfig = ParquetConfig.builder().binaryAsString(false).build();
        final RowMarshaller<ParquetValue> falseMarshaller = ParquetRowMarshaller.newInstance(baseConfig, falseConfig);
        assertThrows(C3rRuntimeException.class, () -> falseMarshaller.marshal());

        // --parquetBinaryAsString is true should cause execution to work on file containing binary values
        final var trueConfig = ParquetConfig.builder().binaryAsString(true).build();
        assertDoesNotThrow(() -> {
            final var marshaller = ParquetRowMarshaller.newInstance(baseConfig, trueConfig);
            marshaller.marshal();
            marshaller.close();
        });

        // Check that fingerprint, sealed and cleartext values were actually written
        final var rows = ParquetTestUtility.readAllRows(output.toString());
        for (var row : rows) {
            final var fingerprintValue = row.getValue(fingerprintHeader);
            assertTrue(fingerprintValue.toString().startsWith(FingerprintTransformer.DESCRIPTOR_PREFIX_STRING));
            final var sealedValue = row.getValue(sealedHeader);
            assertTrue(sealedValue.toString().startsWith(SealedTransformer.DESCRIPTOR_PREFIX_STRING));
            final var cleartextValue = row.getValue(cleartextHeader);
            assertNotNull(cleartextValue.toString());
        }
    }

    @Test
    public void fileCannotBeReadWithGroupColumnType() throws IOException {
        final String input = "../samples/parquet/all_valid_types_and_list.parquet";
        final TableSchema schema = GsonUtil.fromJson(Files.readString(Path.of("../samples/schema/all_valid_types_and_list_schema.json")),
                TableSchema.class);
        final EncryptConfig encryptConfig = EncryptConfig.builder()
                .sourceFile(input)
                .targetFile(output.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(schema)
                .overwrite(true)
                .build();
        assertThrows(C3rRuntimeException.class,
                () -> ParquetRowMarshaller.newInstance(encryptConfig, ParquetConfig.builder().binaryAsString(true).build()));
    }

    @Test
    public void fileCanBeMarshalledWithUnsupportedColumnType() throws IOException {
        final String input = "../samples/parquet/supported_and_unsupported_types.parquet";
        final TableSchema schema = GsonUtil.fromJson(Files.readString(Path.of("../samples/schema/supported_and_unsupported_types.json")),
                TableSchema.class);
        final EncryptConfig encryptConfig = EncryptConfig.builder()
                .sourceFile(input)
                .targetFile(output.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final RowMarshaller<ParquetValue> marshaller = ParquetRowMarshaller.newInstance(encryptConfig,
                ParquetConfig.builder().binaryAsString(true).build());

        assertDoesNotThrow(() -> {
            marshaller.marshal();
            marshaller.close();
        });

        final List<Row<ParquetValue>> rows = ParquetTestUtility.readAllRows(output.toString());
        final Map<ColumnHeader, ParquetValue> expectedValuesForRow0 = Map.ofEntries(
                entry(new ColumnHeader("strings_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:fo5/pWuUN3inaftbnE6a1tAx3JH6TsxC1P8vqIfIRBU=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("small_ints_cleartext"),
                        new ParquetValue.Int32(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_INT_16_TRUE_TYPE),
                                100)),
                entry(new ColumnHeader("small_ints_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:4Hw6YtZFNIYYWbQMpOjdnIOMMzq5Kzzi0DWa3V5NBvg=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("ints_cleartext"),
                        new ParquetValue.Int32(ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_TYPE),
                                100)),
                entry(new ColumnHeader("ints_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:4Hw6YtZFNIYYWbQMpOjdnIOMMzq5Kzzi0DWa3V5NBvg=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("big_ints_cleartext"),
                        new ParquetValue.Int64(ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT64_TYPE),
                                100L)),
                entry(new ColumnHeader("big_ints_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:4Hw6YtZFNIYYWbQMpOjdnIOMMzq5Kzzi0DWa3V5NBvg=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("bools_cleartext"),
                        new ParquetValue.Boolean(ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BOOLEAN_TYPE),
                                true)),
                entry(new ColumnHeader("bools_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:uW2l4SJrRdcch0N+twOptppUcOxpCB9Xe3qkJglhWQI=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("dates32_cleartext"),
                        new ParquetValue.Int32(ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_INT32_DATE_TYPE),
                                99)),
                entry(new ColumnHeader("dates32_fingerprint"),
                        ParquetValue.fromBytes(
                                ParquetDataType.fromType(ParquetTypeDefsTestUtility.SupportedTypes.OPTIONAL_BINARY_STRING_TYPE),
                                "02:hmac:uBJMpqWkIO76V4zO7g20uQiegwbZ9W6zaMns1fcZz7o=".getBytes(StandardCharsets.UTF_8))),
                entry(new ColumnHeader("binary_cleartext"),
                        ParquetValue.fromBytes(ParquetDataType.fromType(ParquetTypeDefsTestUtility.UnsupportedTypes.OPTIONAL_BINARY_TYPE),
                                new byte[]{49, 48, 48}))
        );
        boolean seen = false;
        for (Row<ParquetValue> row : rows) {
            if (row.getValue(new ColumnHeader("strings_cleartext")).toString().equals("100")) {
                seen = true;
                final Set<ColumnHeader> keys = expectedValuesForRow0.keySet();
                for (ColumnHeader header : keys) {
                    final ParquetValue v1 = expectedValuesForRow0.get(header);
                    final ParquetValue v2 = row.getValue(header);
                    assertArrayEquals(v1.getBytes(), v2.getBytes());
                }
            }
        }
        assertTrue(seen);
    }
}
