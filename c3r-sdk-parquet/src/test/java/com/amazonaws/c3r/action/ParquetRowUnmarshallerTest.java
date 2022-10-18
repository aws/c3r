// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static com.amazonaws.c3r.utils.GeneralTestUtility.cleartextColumn;
import static com.amazonaws.c3r.utils.GeneralTestUtility.fingerprintColumn;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_NON_UTF8_DATA_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_NULL_1_ROW_PRIM_DATA_PATH;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_TEST_DATA_HEADERS;
import static com.amazonaws.c3r.utils.ParquetTestUtility.PARQUET_TEST_DATA_TYPES;
import static com.amazonaws.c3r.utils.ParquetTestUtility.readAllRows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetRowUnmarshallerTest {
    private final ClientSettings lowSecurityEncryptNull =
            ClientSettings.builder()
                    .preserveNulls(false)
                    .allowDuplicates(true)
                    .allowJoinsOnColumnsWithDifferentNames(true)
                    .allowCleartext(true)
                    .build();

    private final TableSchema cleartextSchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .map(h -> cleartextColumn(h.toString()))
                    .collect(Collectors.toList()));

    // the string column is the only one that can be encrypted
    private final TableSchema sealedSchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .map(h -> PARQUET_TEST_DATA_TYPES.get(h).equals(ClientDataType.STRING)
                            ? GeneralTestUtility.sealedColumn(h.toString(), PadType.NONE, null)
                            : cleartextColumn(h.toString()))
                    .collect(Collectors.toList()));

    // the string column is the only one that can be HMACd
    private final TableSchema fingerprintSchema = new MappedTableSchema(
            PARQUET_TEST_DATA_HEADERS.stream()
                    .map(h -> PARQUET_TEST_DATA_TYPES.get(h).equals(ClientDataType.STRING)
                            ? fingerprintColumn(h.toString())
                            : cleartextColumn(h.toString()))
                    .collect(Collectors.toList()));

    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    @Test
    public void validateRejectNonParquetFormatTest() {
        final Path decOutput = tempDir.resolve("endToEndMarshalOut.unknown");
        final var configBuilder = DecryptConfig.builder()
                .sourceFile(PARQUET_1_ROW_PRIM_DATA_PATH)
                .targetFile(decOutput.toString())
                .fileFormat(FileFormat.CSV)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .overwrite(true);

        assertThrows(C3rIllegalArgumentException.class, () ->
                ParquetRowUnmarshaller.newInstance(configBuilder.fileFormat(FileFormat.CSV).build()));
        assertDoesNotThrow(() ->
                ParquetRowUnmarshaller.newInstance(configBuilder.fileFormat(FileFormat.PARQUET).build()));
    }

    private void endToEndUnmarshal1RowTest(final String input,
                                           final TableSchema schema,
                                           final ClientSettings settings)
            throws IOException {
        final Path encOutput = tempDir.resolve("endToEndMarshalOut.parquet");

        final var encConfig = EncryptConfig.builder()
                .sourceFile(input)
                .targetFile(encOutput.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir.toAbsolutePath().toString())
                .settings(settings)
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final var marshaller = ParquetRowMarshaller.newInstance(encConfig);

        marshaller.marshal();
        marshaller.close();
        assertNotEquals(0, Files.size(encOutput));

        final Path decOutput = tempDir.resolve("endToEndUnmarshalOut.parquet");

        final var decConfig = DecryptConfig.builder()
                .sourceFile(encOutput.toString())
                .targetFile(decOutput.toString())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .overwrite(true)
                .build();
        final var unmarshaller = ParquetRowUnmarshaller.newInstance(decConfig);

        unmarshaller.unmarshal();
        unmarshaller.close();
        assertNotEquals(0, Files.size(decOutput));

        final List<Row<ParquetValue>> inputRows = readAllRows(input);
        final List<Row<ParquetValue>> marshalledRows = readAllRows(encOutput.toString());
        final List<Row<ParquetValue>> unmarshalledRows = readAllRows(decOutput.toString());

        assertEquals(inputRows.size(), marshalledRows.size());
        assertEquals(inputRows.size(), unmarshalledRows.size());

        for (int i = 0; i < inputRows.size(); i++) {
            final Row<ParquetValue> inputRow = inputRows.get(i);
            final Row<ParquetValue> unmarshalledRow = unmarshalledRows.get(i);

            for (ColumnSchema c : schema.getColumns()) {
                final var inValue = inputRow.getValue(c.getSourceHeader());
                final var outValue = unmarshalledRow.getValue(c.getTargetHeader());
                if (c.getType() != ColumnType.FINGERPRINT) {
                    // CLEARTEXT/SEALED column data: should round trip to the same value always.
                    assertEquals(inValue, outValue);
                } else {
                    // fingerprint column data: only round trips the same if NULL and preserveNULLs is true
                    if (inValue.isNull() && settings.isPreserveNulls()) {
                        assertTrue(outValue.isNull());
                    } else {
                        assertNotEquals(inValue, outValue);
                    }
                }
            }
        }
    }

    @Test
    public void cleartextEndToEndUnmarshal1RowTest() throws IOException {
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, cleartextSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, cleartextSchema, lowSecurityEncryptNull);
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, cleartextSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, cleartextSchema, lowSecurityEncryptNull);
    }

    @Test
    public void sealedEndToEndUnmarshal1RowTest() throws IOException {
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, sealedSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, sealedSchema, lowSecurityEncryptNull);
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, sealedSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, sealedSchema, lowSecurityEncryptNull);
    }

    @Test
    public void sealedEndToEndUnmarshal1RowWithNonUtf8Test() throws IOException {
        endToEndUnmarshal1RowTest(PARQUET_NON_UTF8_DATA_PATH, sealedSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_NON_UTF8_DATA_PATH, sealedSchema, lowSecurityEncryptNull);
        endToEndUnmarshal1RowTest(PARQUET_NON_UTF8_DATA_PATH, sealedSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_NON_UTF8_DATA_PATH, sealedSchema, lowSecurityEncryptNull);
    }

    @Test
    public void joinEndToEndUnmarshal1RowTest() throws IOException {
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, fingerprintSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_1_ROW_PRIM_DATA_PATH, fingerprintSchema, lowSecurityEncryptNull);
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, fingerprintSchema, ClientSettings.lowAssuranceMode());
        endToEndUnmarshal1RowTest(PARQUET_NULL_1_ROW_PRIM_DATA_PATH, fingerprintSchema, lowSecurityEncryptNull);
    }
}
