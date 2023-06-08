// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.CleartextTransformer;
import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;
import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvRowFactory;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.io.CsvRowWriter;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.io.RowReader;
import com.amazonaws.c3r.io.RowReaderTestUtility;
import com.amazonaws.c3r.io.RowWriter;
import com.amazonaws.c3r.io.SqlRowReader;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.utils.GeneralTestUtility.CONFIG_SAMPLE;
import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static com.amazonaws.c3r.utils.GeneralTestUtility.cleartextColumn;
import static com.amazonaws.c3r.utils.GeneralTestUtility.sealedColumn;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowMarshallerTest {

    private String tempDir;

    private String output;

    private Map<ColumnType, Transformer> transformers;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = FileTestUtility.createTempDir().toString();
        output = FileTestUtility.resolve("output.csv").toString();
        final Encryptor encryptor = Encryptor.getInstance(new SymmetricStaticProvider(TEST_CONFIG_DATA_SAMPLE.getKey(),
                TEST_CONFIG_DATA_SAMPLE.getSalt().getBytes(StandardCharsets.UTF_8)));
        transformers = new HashMap<>();
        transformers.put(ColumnType.CLEARTEXT, new CleartextTransformer());
        transformers.put(ColumnType.FINGERPRINT, new FingerprintTransformer(TEST_CONFIG_DATA_SAMPLE.getKey(),
                TEST_CONFIG_DATA_SAMPLE.getSalt().getBytes(StandardCharsets.UTF_8), TEST_CONFIG_DATA_SAMPLE.getSettings(), false));
        transformers.put(ColumnType.SEALED, new SealedTransformer(encryptor, TEST_CONFIG_DATA_SAMPLE.getSettings()));
    }

    @Test
    public void csvRowMarshallerNewInstanceTest() {
        final var marshaller = CsvRowMarshaller.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
                .tempDir(tempDir)
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .schema(TEST_CONFIG_DATA_SAMPLE.getSchema())
                .transforms(transformers).build();
        assertNotNull(marshaller);
        // compare input as sets since ordering isn't guaranteed and duplicates can exist
        // via one-to-many mappings
        final var expected = TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns().stream()
                .map(ColumnSchema::getSourceHeader).map(ColumnHeader::toString).sorted().collect(Collectors.toList());
        final var actual = marshaller.getInputReader().getHeaders().stream()
                .map(ColumnHeader::toString).sorted().collect(Collectors.toList());
        assertTrue(expected.removeAll(actual));
        assertTrue(expected.isEmpty());
        assertEquals(Set.of(TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns().stream()
                        .map(ColumnSchema::getTargetHeader).map(ColumnHeader::toString).sorted().toArray()),
                Set.of(marshaller.getOutputWriter().getHeaders().stream().map(ColumnHeader::toString).sorted().toArray()));
        assertEquals(Set.of(marshaller.getColumnInsights().stream()
                        .map(ColumnSchema::getTargetHeader).map(ColumnHeader::toString).sorted().toArray()),
                Set.of(marshaller.getOutputWriter().getHeaders().stream().map(ColumnHeader::toString).sorted().toArray()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void respectPadTypesTest() {
        final RowWriter<CsvValue> rowWriter = (RowWriter<CsvValue>) mock(RowWriter.class);
        final RowMarshaller<CsvValue> marshaller = RowMarshaller.<CsvValue>builder()
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .schema(TEST_CONFIG_DATA_SAMPLE.getSchema())
                .inputReader(CsvRowReader.builder().sourceName(TEST_CONFIG_DATA_SAMPLE.getInput()).build())
                .rowFactory(new CsvRowFactory())
                .outputWriter(rowWriter)
                .transformers(transformers)
                .tempDir(tempDir)
                .build();
        marshaller.loadInput();

        final Map<ColumnHeader, ColumnInsight> targetHeaderMappedColumnInsights = marshaller.getColumnInsights().stream()
                .collect(Collectors.toMap(ColumnSchema::getTargetHeader, Function.identity()));

        final var notesTargetHeader = new ColumnHeader("notes");

        final int longestNoteValueByteLength = 60;

        if (!FileUtil.isWindows()) {
            // NOTE 1: Spot check our length ONLY on *nix system CI. On Windows the length of Java string literals appearing
            // in tests like this be encoded differently. This only matters for tests like this storing
            // string literals - it does not matter when we read in a file from disk that is UTF8.
            // NOTE 2: Importantly, the longest `Notes` string has a unicode character `é` (U+00E9) that takes two bytes
            // in UTF8 (0xC3 0xA9), and so relying on non-UTF8-byte-length notions of a string value's "length"
            // can lead to errors on UTF8 data containing such values.
            assertEquals(
                    longestNoteValueByteLength,
                    "This is a really long noté that could really be a paragraph"
                            .getBytes(StandardCharsets.UTF_8).length);
        }

        assertEquals(
                longestNoteValueByteLength,
                targetHeaderMappedColumnInsights.get(notesTargetHeader).getMaxValueLength());
    }

    @Test
    public void loadInputTest() {
        final RowMarshaller<CsvValue> marshaller = CsvRowMarshaller.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
                .tempDir(tempDir)
                .settings(ClientSettings.lowAssuranceMode())
                .schema(TEST_CONFIG_DATA_SAMPLE.getSchema())
                .transforms(transformers).build();
        marshaller.loadInput();
        final SqlRowReader<CsvValue> reader = new SqlRowReader<>(
                marshaller.getColumnInsights(),
                marshaller.getNonceHeader(),
                new CsvRowFactory(),
                marshaller.getSqlTable());
        // Ensure columns were encrypted/HMACed where appropriate.
        while (reader.hasNext()) {
            final Row<CsvValue> row = reader.next();
            for (ColumnSchema columnSchema : TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns()) {
                if (columnSchema.getType() == ColumnType.CLEARTEXT) {
                    continue; // Cleartext columns weren't touched.
                }
                final CsvValue value = row.getValue(columnSchema.getTargetHeader());
                if (value.getBytes() == null) {
                    continue; // Null values weren't touched
                }
                final Transformer transformer = transformers.get(columnSchema.getType());
                assertTrue(Transformer.hasDescriptor(transformer, value.getBytes()));
            }
        }
        assertNotEquals(0, marshaller.getInputReader().getReadRowCount()); // Ensure data was read
        assertEquals(marshaller.getInputReader().getReadRowCount(), reader.getReadRowCount()); // Ensure table data matches data read
    }

    @Test
    public void loadInputOmitsColumnsNotInConfigTest() {
        final ColumnSchema toOmit = TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns().get(0);
        final List<ColumnSchema> columnSchemas = new ArrayList<>(TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns());
        columnSchemas.remove(toOmit);
        final TableSchema tableSchema = new MappedTableSchema(columnSchemas);
        final RowMarshaller<CsvValue> marshaller = CsvRowMarshaller.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
                .tempDir(tempDir)
                .settings(ClientSettings.lowAssuranceMode())
                .schema(tableSchema)
                .transforms(transformers).build();
        marshaller.loadInput();
        final SqlRowReader<CsvValue> reader = new SqlRowReader<>(
                marshaller.getColumnInsights(),
                marshaller.getNonceHeader(),
                new CsvRowFactory(),
                marshaller.getSqlTable());
        while (reader.hasNext()) {
            assertFalse(reader.next().hasColumn(toOmit.getTargetHeader()));
        }
        assertNotEquals(0, marshaller.getInputReader().getReadRowCount()); // Ensure data was read
        assertEquals(marshaller.getInputReader().getReadRowCount(), reader.getReadRowCount()); // Ensure table data matches data read
    }

    @Test
    public void missingInputColumnsThrowsTest() {
        final ColumnSchema missingCol = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("missing_column"))
                .type(ColumnType.CLEARTEXT).build();
        final List<ColumnSchema> columnSchemas = new ArrayList<>(TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns());
        columnSchemas.add(missingCol);

        final TableSchema tableSchema = new MappedTableSchema(columnSchemas);
        assertThrows(C3rRuntimeException.class, () -> CsvRowMarshaller.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
                .tempDir(tempDir)
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .schema(tableSchema)
                .transforms(transformers).build());
    }

    @Test
    public void marshalTransformerFailureTest() {
        final SealedTransformer badSealedTransformer = mock(SealedTransformer.class);
        when(badSealedTransformer.marshal(any(), any())).thenThrow(new C3rRuntimeException("error"));
        transformers.put(ColumnType.SEALED, badSealedTransformer);

        final RowWriter<CsvValue> rowWriter = CsvRowWriter.builder()
                .targetName(output)
                .headers(CONFIG_SAMPLE.getColumns().stream().map(ColumnSchema::getTargetHeader).collect(Collectors.toList()))
                .build();
        final RowMarshaller<CsvValue> marshaller = RowMarshaller.<CsvValue>builder()
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .schema(TEST_CONFIG_DATA_SAMPLE.getSchema())
                .inputReader(CsvRowReader.builder().sourceName(TEST_CONFIG_DATA_SAMPLE.getInput()).build())
                .rowFactory(new CsvRowFactory())
                .outputWriter(rowWriter)
                .transformers(transformers)
                .tempDir(tempDir)
                .build();
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
    }

    @Test
    public void endToEndMarshallingTest() {
        final RowWriter<CsvValue> rowWriter = CsvRowWriter.builder()
                .targetName(output)
                .headers(TEST_CONFIG_DATA_SAMPLE.getSchema().getColumns().stream().map(ColumnSchema::getTargetHeader)
                        .collect(Collectors.toList()))
                .build();
        final RowMarshaller<CsvValue> marshaller = RowMarshaller.<CsvValue>builder()
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .schema(TEST_CONFIG_DATA_SAMPLE.getSchema())
                .inputReader(CsvRowReader.builder().sourceName(TEST_CONFIG_DATA_SAMPLE.getInput()).build())
                .rowFactory(new CsvRowFactory())
                .outputWriter(rowWriter)
                .transformers(transformers)
                .tempDir(tempDir)
                .build();
        marshaller.marshal();
        final String file = FileUtil.readBytes(output);
        assertFalse(file.isBlank());
        marshaller.close();
    }

    @Test
    public void roundTripCsvTest() throws IOException {
        final var headers = List.of("FirstName", "LastName", "Address", "City", "State", "PhoneNumber", "Title", "Level", "Notes");
        final var columnSchemas = new ArrayList<ColumnSchema>();
        for (var header : headers) {
            columnSchemas.add(cleartextColumn(header));
            columnSchemas.add(sealedColumn(header, header + "_sealed"));
            columnSchemas.add(sealedColumn(header, header + "_fixed", PadType.FIXED, 100));
            columnSchemas.add(sealedColumn(header, header + "_max", PadType.MAX, 50));
        }
        final TableSchema schema = new MappedTableSchema(columnSchemas);
        final String ciphertextFile = FileTestUtility.resolve("roundTripCsvCipherOut.csv").toString();
        final String cleartextFile = FileTestUtility.resolve("roundTripCsvPlainOut.csv").toString();

        final EncryptConfig encryptConfig = EncryptConfig.builder()
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(ciphertextFile)
                .tempDir(tempDir)
                .overwrite(true)
                .csvInputNullValue(null)
                .csvOutputNullValue(null)
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .settings(ClientSettings.builder()
                        .allowCleartext(true)
                        .allowDuplicates(false)
                        .allowJoinsOnColumnsWithDifferentNames(false)
                        .preserveNulls(false)
                        .build())
                .tableSchema(schema)
                .build();
        final RowMarshaller<CsvValue> rowMarshaller = CsvRowMarshaller.newInstance(encryptConfig);
        rowMarshaller.marshal();
        rowMarshaller.close();

        final var originalData = CsvTestUtility.readRows(TEST_CONFIG_DATA_SAMPLE.getInput()).stream()
                .sorted(Comparator.comparing(v -> v.get("FirstName")))
                .collect(Collectors.toList());
        final var cipherData = CsvTestUtility.readRows(ciphertextFile).stream()
                .sorted(Comparator.comparing(v -> v.get("firstname")))
                .collect(Collectors.toList());

        assertEquals(originalData.size(), cipherData.size());
        for (int i = 0; i < originalData.size(); i++) {
            final var originalLine = originalData.get(i);
            final var cipherLine = cipherData.get(i);
            assertEquals(originalLine.size() * 4, cipherLine.size());
            for (String s : headers) {
                final var original = originalLine.get(s);
                final var cipherCleartext = cipherLine.get(new ColumnHeader(s).toString());
                assertEquals(original, cipherCleartext);
                final var cipherSealed = cipherLine.get(s + "_sealed");
                assertNotEquals(original, cipherSealed);
                final var cipherFixed = cipherLine.get(s + "_fixed");
                assertNotEquals(original, cipherFixed);
                final var cipherMax = cipherLine.get(s + "_max");
                assertNotEquals(original, cipherMax);
            }
        }

        final DecryptConfig decryptConfig = DecryptConfig.builder()
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile(ciphertextFile)
                .targetFile(cleartextFile)
                .overwrite(true)
                .csvInputNullValue(null)
                .csvOutputNullValue(null)
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .failOnFingerprintColumns(true)
                .build();
        final RowUnmarshaller<CsvValue> rowUnmarshaller = CsvRowUnmarshaller.newInstance(decryptConfig);
        rowUnmarshaller.unmarshal();
        rowUnmarshaller.close();

        final var finalData =
                CsvTestUtility.readRows(cleartextFile).stream().sorted(Comparator.comparing(v -> v.get("firstname")))
                        .collect(Collectors.toList());
        assertEquals(originalData.size(), finalData.size());
        for (int i = 0; i < originalData.size(); i++) {
            final var originalLine = originalData.get(i);
            final var resultLine = finalData.get(i);
            assertEquals(originalLine.size() * 4, resultLine.size());
            for (String s : headers) {
                final var resultHeader = new ColumnHeader(s).toString();
                final var original = originalLine.get(s);
                final var result = resultLine.get(resultHeader);
                assertEquals(original, result);
                final var cipherSealed = resultLine.get(resultHeader + "_sealed");
                assertEquals(original, cipherSealed);
                final var cipherFixed = resultLine.get(resultHeader + "_fixed");
                assertEquals(original, cipherFixed);
                final var cipherMax = resultLine.get(resultHeader + "_max");
                assertEquals(original, cipherMax);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadManyRowsTest() {
        final RowReader<CsvValue> mockReader = RowReaderTestUtility.getMockCsvReader(
                List.of(new ColumnHeader("cleartext")),
                (header) -> header + " value",
                RowMarshaller.LOG_ROW_UPDATE_FREQUENCY - RowMarshaller.INSERTS_PER_COMMIT, // read rows
                RowMarshaller.LOG_ROW_UPDATE_FREQUENCY + 1 // total rows
        );
        final RowWriter<CsvValue> mockWriter = (RowWriter<CsvValue>) mock(RowWriter.class);
        final TableSchema schema = new MappedTableSchema(List.of(cleartextColumn("cleartext")));
        final RowMarshaller<CsvValue> marshaller = RowMarshaller.<CsvValue>builder()
                .inputReader(mockReader)
                .outputWriter(mockWriter)
                .tempDir(FileUtil.TEMP_DIR)
                .schema(schema)
                .rowFactory(new CsvRowFactory())
                .transformers(transformers)
                .settings(ClientSettings.lowAssuranceMode())
                .build();
        assertDoesNotThrow(marshaller::marshal);
    }

    // Test if duplicate entries in a fingerprint column throws an error when expected.
    @SuppressWarnings("unchecked")
    private void validateDuplicateEnforcement(final Function<ColumnHeader, String> valueProducer,
                                              final boolean allowDuplicates) {
        final RowReader<CsvValue> mockReader = RowReaderTestUtility.getMockCsvReader(
                List.of(new ColumnHeader("some fingerprint column")),
                valueProducer,
                0, // read rows
                10 // total rows
        );

        final var clientSettings = ClientSettings.builder()
                .allowDuplicates(allowDuplicates)
                .preserveNulls(false)
                .allowJoinsOnColumnsWithDifferentNames(true)
                .allowCleartext(true)
                .build();

        final RowWriter<CsvValue> mockWriter = (RowWriter<CsvValue>) mock(RowWriter.class);
        final TableSchema schema = new MappedTableSchema(List.of(GeneralTestUtility.fingerprintColumn("some fingerprint column")));
        final RowMarshaller<CsvValue> marshaller = RowMarshaller.<CsvValue>builder()
                .inputReader(mockReader)
                .outputWriter(mockWriter)
                .tempDir(FileUtil.TEMP_DIR)
                .schema(schema)
                .rowFactory(new CsvRowFactory())
                .transformers(transformers)
                .settings(clientSettings)
                .build();
        if (allowDuplicates) {
            assertDoesNotThrow(marshaller::marshal);
        } else {
            assertThrows(C3rRuntimeException.class, marshaller::marshal);
        }
    }

    @Test
    public void validateDuplicateEnforcementTest() {
        // check that multiple duplicate fingerprint values get rejected
        validateDuplicateEnforcement((header) -> "duplicate value", true);
        validateDuplicateEnforcement((header) -> "duplicate value", false);
        // check that multiple preserved null values get rejected
        validateDuplicateEnforcement((header) -> null, true);
        validateDuplicateEnforcement((header) -> null, false);
    }
}
