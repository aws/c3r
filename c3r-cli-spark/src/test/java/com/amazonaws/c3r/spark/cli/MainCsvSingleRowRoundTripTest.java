// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;
import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.spark.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.spark.io.CsvTestUtility;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.GeneralTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/*
 * A test class with a single row of data containing self-descriptively named column entries
 * across the gambit of possible kinds of data that could appear. Intended to act
 * as easy to audit unit tests for round tripping through the C3R with various settings and CSV input/output.
 */
public class MainCsvSingleRowRoundTripTest {
    // ColumnSchema Name -> ColumnSchema Value mappings used for convenient testing data
    // written out to a CSV file and then parsed in
    private final List<Map.Entry<String, String>> exampleCsvEntries = List.of(
            Map.entry("foo", "foo"),
            Map.entry("quoted-foo", "\"foo\""),
            Map.entry("quoted-foo-newline-bar", "\"foo\nbar\""),
            Map.entry("blank", ""), // `,,`
            Map.entry("1space", " "), // `, ,`
            Map.entry("quoted-blank", "\"\""),
            Map.entry("quoted-1space", "\" \"")
    );

    private EncryptCliConfigTestUtility encArgs;

    private DecryptCliConfigTestUtility decArgs;

    private String encCsvInputNull;

    private String encCsvOutputNull;

    private String decCsvInputNull;

    private String decCsvOutputNull;

    private Path input;

    private ColumnSchema createColumn(final String headerName, final ColumnType type, final Pad pad) {
        final var columnBuilder = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader(headerName))
                .targetHeader(new ColumnHeader(headerName))
                .type(type);
        if (type == ColumnType.SEALED) {
            columnBuilder.pad(pad);
        }
        return columnBuilder.build();
    }

    // Create a schema where all columns have the same type and padding.
    private TableSchema createMonoSchema(final ColumnType type, final Pad pad) {
        if (type != ColumnType.SEALED && pad != null) {
            throw new C3rRuntimeException("Bad test! Can't pad non-sealed columns!");
        }

        return new MappedTableSchema(exampleCsvEntries.stream()
                .map(entry -> createColumn(entry.getKey(), type, pad))
                .collect(Collectors.toList())
        );
    }

    @BeforeEach
    public void setup() throws IOException {
        input = FileTestUtility.createTempFile("csv-values", ".csv");
        final String headerRow = exampleCsvEntries.stream().map(Map.Entry::getKey).collect(Collectors.joining(","));
        final String valueRow = exampleCsvEntries.stream().map(Map.Entry::getValue).collect(Collectors.joining(","));
        Files.writeString(input,
                String.join("\n",
                        headerRow,
                        valueRow));

        encArgs = EncryptCliConfigTestUtility.blankTestArgs();
        decArgs = DecryptCliConfigTestUtility.blankTestArgs();

        encCsvInputNull = null;
        encCsvOutputNull = null;
        decCsvInputNull = null;
        decCsvOutputNull = null;
    }

    private String encrypt(final ColumnType type, final Pad pad) throws IOException {
        final String output = FileTestUtility.createTempDir().toString();
        final Path schemaPath = FileTestUtility.createTempFile("schema", ".json");
        schemaPath.toFile().deleteOnExit();

        final var writer = Files.newBufferedWriter(schemaPath, StandardCharsets.UTF_8);
        writer.write(GsonUtil.toJson(createMonoSchema(type, pad)));
        writer.close();

        encArgs.setInput(input.toString());
        encArgs.setAllowCleartext(true);
        encArgs.setEnableStackTraces(true);
        encArgs.setSchema(schemaPath.toString());
        encArgs.setCollaborationId(GeneralTestUtility.EXAMPLE_SALT.toString());
        encArgs.setOutput(output);
        encArgs.setOverwrite(true);
        if (encCsvInputNull != null) {
            encArgs.setCsvInputNullValue(encCsvInputNull);
        }
        if (encCsvOutputNull != null) {
            encArgs.setCsvOutputNullValue(encCsvOutputNull);
        }

        final CleanRoomsDao cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        final int exitCode = EncryptMode.getApp(cleanRoomsDao, SparkSessionTestUtility.initSparkSession())
                .execute(encArgs.toArrayWithoutMode());
        assertEquals(0, exitCode);

        return CsvTestUtility.mergeOutput(Path.of(output)).toString();
    }

    private String encryptAllColumnsCleartext() throws IOException {
        return encrypt(ColumnType.CLEARTEXT, null);
    }

    private String encryptAllColumnsSealed() throws IOException {
        return encrypt(ColumnType.SEALED, Pad.DEFAULT);
    }

    private String encryptAllColumnsFingerprint() throws IOException {
        return encrypt(ColumnType.FINGERPRINT, null);
    }

    private String decrypt(final String inPath) throws IOException {
        final String output = FileTestUtility.createTempDir().toString();

        decArgs.setInput(inPath);
        decArgs.setFailOnFingerprintColumns(false);
        decArgs.setEnableStackTraces(true);
        decArgs.setCollaborationId(GeneralTestUtility.EXAMPLE_SALT.toString());
        decArgs.setOutput(output);
        decArgs.setOverwrite(true);
        if (decCsvInputNull != null) {
            decArgs.setCsvInputNullValue(decCsvInputNull);
        }
        if (decCsvOutputNull != null) {
            decArgs.setCsvOutputNullValue(decCsvOutputNull);
        }

        final int exitCode = DecryptMode.getApp(SparkSessionTestUtility.initSparkSession()).execute(decArgs.toArrayWithoutMode());
        assertEquals(0, exitCode);

        return CsvTestUtility.mergeOutput(Path.of(output)).toString();
    }

    private Map<String, String> readSingleCsvRow(final String path) {
        final var rows = CsvTestUtility.readRows(path);
        assertEquals(1, rows.size());
        return rows.get(0);
    }

    public void validateCleartextRoundTripEncDecRowContent(final Map<String, Predicate<String>> expectedEncRow,
                                                           final Map<String, Predicate<String>> expectedDecRow) throws IOException {
        final String encryptedPath = encryptAllColumnsCleartext();
        final var rowPostEncryption = readSingleCsvRow(encryptedPath);
        GeneralTestUtility.assertRowEntryPredicates(rowPostEncryption, expectedEncRow);

        final String decryptedPath = decrypt(encryptedPath);
        final var rowPostDecryption = readSingleCsvRow(decryptedPath);
        GeneralTestUtility.assertRowEntryPredicates(rowPostDecryption, expectedDecRow);
    }

    public void validateSealedRoundTripDecRowContent(final Map<String, Predicate<String>> expectedDecRow) throws IOException {
        final String encryptedPath = encryptAllColumnsSealed();
        final var rowPostEncryption = readSingleCsvRow(encryptedPath);
        assertTrue(rowPostEncryption.values().stream().map((val) -> val.startsWith(SealedTransformer.DESCRIPTOR_PREFIX_STRING))
                .dropWhile((val) -> val).collect(Collectors.toSet()).isEmpty());

        final String decryptedPath = decrypt(encryptedPath);
        final var rowPostDecryption = readSingleCsvRow(decryptedPath);
        GeneralTestUtility.assertRowEntryPredicates(rowPostDecryption, expectedDecRow);
    }

    @Test
    public void defaultEncNulls_defaultDecNulls_EncDec_CleartextTest() throws IOException {
        final Map<String, Predicate<String>> expectedEncRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("foo")),
                entry("quoted-foo", (val) -> val.equals("foo")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateCleartextRoundTripEncDecRowContent(expectedEncRow, expectedEncRow);
    }

    @Test
    public void customEncNulls_EncDec_CleartextTest() throws IOException {
        encCsvInputNull = "foo";
        encCsvOutputNull = "bar";

        final Map<String, Predicate<String>> expectedEncRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("bar")),
                entry("quoted-foo", (val) -> val.equals("bar")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateCleartextRoundTripEncDecRowContent(expectedEncRow, expectedEncRow);
    }

    @Test
    public void customNulls_Dec_CleartextTest() throws IOException {
        decCsvInputNull = "foo";
        decCsvOutputNull = "bar";

        final Map<String, Predicate<String>> expectedEncryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("foo")),
                entry("quoted-foo", (val) -> val.equals("foo")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("bar")),
                entry("quoted-foo", (val) -> val.equals("bar")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateCleartextRoundTripEncDecRowContent(expectedEncryptRow, expectedDecryptRow);
    }

    @Test
    public void customNulls_EncDec_CleartextTest() throws IOException {
        encCsvInputNull = "foo";
        encCsvOutputNull = "bar";
        decCsvInputNull = "bar";
        decCsvOutputNull = "baz";

        final Map<String, Predicate<String>> expectedEncryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("bar")),
                entry("quoted-foo", (val) -> val.equals("bar")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("baz")),
                entry("quoted-foo", (val) -> val.equals("baz")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                // spaces are trimmed on unquoted input, so we again get `""` i.e. NULL
                entry("1space", (val) -> val.equals("")),
                // by default, a blank and a quoted blank both are treated as NULL
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateCleartextRoundTripEncDecRowContent(expectedEncryptRow, expectedDecryptRow);
    }

    @Test
    public void defaultEncNulls_defaultDecNulls_EncDec_SealedTest() throws IOException {
        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("foo")),
                entry("quoted-foo", (val) -> val.equals("foo")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                entry("1space", (val) -> val.equals("")),
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateSealedRoundTripDecRowContent(expectedDecryptRow);
    }

    @Test
    public void customEncNulls_defaultDecOutNull_EncDec_SealedTest() throws IOException {
        encCsvInputNull = "foo";
        encCsvOutputNull = "bar";

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                // encrypted as NULL
                entry("foo", (val) -> val.equals("")),
                // encrypted as NULL
                entry("quoted-foo", (val) -> val.equals("")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                // written as `,"",` since default NULL encoding `,,` is being used
                entry("blank", (val) -> val.equals("\"\"")),
                // written as `,"",` since default NULL encoding `,,` is being used
                entry("1space", (val) -> val.equals("\"\"")),
                // written as `,"",` since default NULL encoding `,,` is being used
                entry("quoted-blank", (val) -> val.equals("\"\"")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateSealedRoundTripDecRowContent(expectedDecryptRow);
    }

    @Test
    public void customEncNulls_customDecOutNull_EncDec_SealedTest() throws IOException {
        encCsvInputNull = "foo";
        encCsvOutputNull = "bar";
        decCsvOutputNull = "baz";

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("baz")),
                entry("quoted-foo", (val) -> val.equals("baz")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                entry("1space", (val) -> val.equals("")),
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateSealedRoundTripDecRowContent(expectedDecryptRow);
    }

    @Test
    public void defaultEncNulls_customDecNulls_EncDec_SealedTest() throws IOException {
        decCsvInputNull = "";
        decCsvOutputNull = "baz";

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("foo")),
                entry("quoted-foo", (val) -> val.equals("foo")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("baz")),
                entry("1space", (val) -> val.equals("baz")),
                entry("quoted-blank", (val) -> val.equals("baz")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateSealedRoundTripDecRowContent(expectedDecryptRow);
    }

    @Test
    public void customEncNulls_customDecNulls_EncDec_SealedTest() throws IOException {
        encCsvInputNull = "foo";
        encCsvOutputNull = "Aliens";
        decCsvInputNull = " ";
        decCsvOutputNull = "Zombies, run!";

        final Map<String, Predicate<String>> expectedDecryptRow = Map.ofEntries(
                entry("foo", (val) -> val.equals("\"Zombies, run!\"")),
                entry("quoted-foo", (val) -> val.equals("\"Zombies, run!\"")),
                entry("quoted-foo-newline-bar", (val) -> val.equals("\"foo\nbar\"")),
                entry("blank", (val) -> val.equals("")),
                entry("1space", (val) -> val.equals("")),
                entry("quoted-blank", (val) -> val.equals("")),
                // quotes do not preserve leading/trailing space
                entry("quoted-1space", (val) -> val.equals("\" \""))
        );

        validateSealedRoundTripDecRowContent(expectedDecryptRow);
    }

    public void defaultNull_EncDec_Fingerprint(final boolean allowJoinsOnColumnsWithDifferentNames) throws IOException {
        encArgs.setAllowJoinsOnColumnsWithDifferentNames(allowJoinsOnColumnsWithDifferentNames);
        final String encryptedPath = encryptAllColumnsFingerprint();
        final var rowPostEncryption = readSingleCsvRow(encryptedPath);
        final Predicate<String> isFingerprintEncrypted = (val) -> val.startsWith(FingerprintTransformer.DESCRIPTOR_PREFIX_STRING);
        GeneralTestUtility.assertRowEntryPredicates(rowPostEncryption,
                entry("foo", isFingerprintEncrypted),
                entry("quoted-foo", isFingerprintEncrypted),
                entry("quoted-foo-newline-bar", isFingerprintEncrypted),
                entry("blank", isFingerprintEncrypted),
                entry("1space", isFingerprintEncrypted),
                entry("quoted-blank", isFingerprintEncrypted),
                entry("quoted-1space", isFingerprintEncrypted)
        );

        // check non-NULL values (`foo` and `"foo"`) get the same encoding
        // iff allowJoinsOnColumnsWithDifferentNames is true
        if (allowJoinsOnColumnsWithDifferentNames) {
            assertEquals(
                    rowPostEncryption.get("foo"),
                    rowPostEncryption.get("quoted-foo"));
        } else {
            assertNotEquals(
                    rowPostEncryption.get("foo"),
                    rowPostEncryption.get("quoted-foo"));
        }

        // ensure we always transform NULL to unique values to preserve
        // the "uniqueness" of NULLs w.r.t. SQL semantics
        assertNotEquals(
                rowPostEncryption.get("blank"),
                rowPostEncryption.get("1space"));
        assertNotEquals(
                rowPostEncryption.get("blank"),
                rowPostEncryption.get("quoted-blank"));

        // fingerprint values don't get decrypted
        final String decryptedPath = decrypt(encryptedPath);
        final var rowPostDecryption = readSingleCsvRow(decryptedPath);
        GeneralTestUtility.assertRowEntryPredicates(rowPostDecryption,
                entry("foo", isFingerprintEncrypted),
                entry("quoted-foo", isFingerprintEncrypted),
                entry("quoted-foo-newline-bar", isFingerprintEncrypted),
                entry("blank", isFingerprintEncrypted),
                entry("1space", isFingerprintEncrypted),
                entry("quoted-blank", isFingerprintEncrypted),
                entry("quoted-1space", isFingerprintEncrypted)
        );
    }

    @Test
    public void defaultNull_EncDec_allowJoinsOnColumnsWithDifferentNamesIsTrue_FingerprintTest() throws IOException {
        defaultNull_EncDec_Fingerprint(true);
    }

    @Test
    public void defaultNull_EncDec_allowJoinsOnColumnsWithDifferentNamesIsFalse_FingerprintTest() throws IOException {
        defaultNull_EncDec_Fingerprint(false);
    }

    public void blankEncNull_Fingerprint(final boolean allowJoinsOnColumnsWithDifferentNames) throws IOException {
        encCsvInputNull = "";
        encArgs.setAllowJoinsOnColumnsWithDifferentNames(allowJoinsOnColumnsWithDifferentNames);

        final String encryptedPath = encryptAllColumnsFingerprint();
        final var rowPostEncryption = readSingleCsvRow(encryptedPath);
        final Predicate<String> isFingerprintEncrypted = (val) -> val.startsWith(FingerprintTransformer.DESCRIPTOR_PREFIX_STRING);
        GeneralTestUtility.assertRowEntryPredicates(rowPostEncryption,
                entry("foo", isFingerprintEncrypted),
                entry("quoted-foo", isFingerprintEncrypted),
                entry("quoted-foo-newline-bar", isFingerprintEncrypted),
                entry("blank", isFingerprintEncrypted),
                entry("1space", isFingerprintEncrypted),
                entry("quoted-blank", isFingerprintEncrypted),
                entry("quoted-1space", isFingerprintEncrypted)
        );

        // check that NULL values never get the same encoding
        // (preserving NULL "uniqueness" for fingerprint columns)
        assertNotEquals(
                rowPostEncryption.get("blank"),
                rowPostEncryption.get("1space"));

        // check that `,,` and and `,"",` get different encoding when the user
        // specifies `""` as the input NULL value
        assertNotEquals(
                rowPostEncryption.get("blank"),
                rowPostEncryption.get("quoted-blank"));
    }

    @Test
    public void blankEncNull_Enc_allowJoinsOnColumnsWithDifferentNamesIsTrue_FingerprintTest() throws IOException {
        blankEncNull_Fingerprint(true);

    }

    @Test
    public void blankEncNull_Enc_allowJoinsOnColumnsWithDifferentNamesIsFalse_FingerprintTest() throws IOException {
        blankEncNull_Fingerprint(false);
    }

    @Test
    public void emptyQuotesEncNull_FingerprintTest() throws IOException {
        encCsvInputNull = "\"\"";
        final String encryptedPath = encryptAllColumnsFingerprint();
        final var rowPostEncryption = readSingleCsvRow(encryptedPath);
        final Predicate<String> isFingerprintEncrypted = (val) -> val.startsWith(FingerprintTransformer.DESCRIPTOR_PREFIX_STRING);
        GeneralTestUtility.assertRowEntryPredicates(rowPostEncryption,
                entry("foo", isFingerprintEncrypted),
                entry("quoted-foo", isFingerprintEncrypted),
                entry("quoted-foo-newline-bar", isFingerprintEncrypted),
                entry("blank", isFingerprintEncrypted),
                entry("1space", isFingerprintEncrypted),
                entry("quoted-blank", isFingerprintEncrypted),
                entry("quoted-1space", isFingerprintEncrypted)
        );

        // check that `,"",` and `," ",` get different encodings
        assertNotEquals(
                rowPostEncryption.get("quoted-blank"),
                rowPostEncryption.get("quoted-1space"));
        // check that `,,` and and `,"",` get different encoding when the user
        // specifies `""` as the input NULL value
        assertNotEquals(
                rowPostEncryption.get("blank"),
                rowPostEncryption.get("quoted-blank"));
    }
}
