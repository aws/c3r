// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.io.ParquetTestUtility;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MainTest {
    private final String config = "../samples/schema/config_sample.json";

    private EncryptCliConfigTestUtility encArgs;

    private DecryptCliConfigTestUtility decArgs;

    private CleanRoomsDao cleanRoomsDao;

    private Path output;

    // Reset encryption and decryption command line arguments before each test
    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.resolve("output.csv");

        encArgs = EncryptCliConfigTestUtility.defaultTestArgs();
        encArgs.setSchema(config);
        encArgs.setAllowDuplicates(true);

        decArgs = DecryptCliConfigTestUtility.defaultTestArgs();
        decArgs.setFailOnFingerprintColumns(false);

        cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
    }

    // Verify calling the command with no argument fails
    @Test
    public void noArgsUsageTest() {
        final ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        System.setErr(new PrintStream(consoleOutput));
        final int exitCode = Main.getApp().execute();
        assertEquals(2, exitCode);
        assertTrue(consoleOutput.toString().toLowerCase().contains("missing required subcommand"));
    }

    // Make sure help is printed out
    @Test
    public void helpTest() {
        ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        System.setErr(new PrintStream(consoleOutput));
        consoleOutput = new ByteArrayOutputStream();
        System.setOut(new PrintStream(consoleOutput));
        Main.getApp().execute("--help");
        assertTrue(consoleOutput.toString().toLowerCase().contains("usage"));
    }

    // Make sure a bad subcommand isn't accepted
    @Test
    public void validateCommandBadTest() {
        final ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        System.setErr(new PrintStream(consoleOutput));
        final int exitCode = Main.getApp().execute("fly-to-the-moon");
        assertEquals(2, exitCode);
    }

    // Test to make sure quotes are removed
    @Test
    public void quotesAreRemovedTest() {
        final String[] args = {"encrypt", "C:\\User Name\\Here", "--schema=\"schema\"", "--id=\"" + GeneralTestUtility.EXAMPLE_SALT + "\"",
                "--overwrite=\"true\""};
        final CommandLine.ParseResult pr = CliTestUtility.verifyCliOptions(args);
        final List<String> origArgs = pr.subcommands().get(0).originalArgs();
        assertArrayEquals(args, origArgs.toArray(String[]::new));
        final List<String> parsedArgs = pr.subcommands().get(0).matchedArgs().stream()
                .map(CommandLine.Model.ArgSpec::getValue).map(Object::toString).collect(Collectors.toList());
        assertEquals("encrypt", pr.subcommands().get(0).commandSpec().name());
        assertEquals("C:\\User Name\\Here", parsedArgs.get(0));
        assertEquals("schema", parsedArgs.get(1));
        assertEquals(GeneralTestUtility.EXAMPLE_SALT.toString(), parsedArgs.get(2));
        assertTrue(Boolean.parseBoolean(parsedArgs.get(3)));
    }

    // Check the encrypt command to make sure it works as expected
    @Test
    public void marshalTest() throws IOException {
        encArgs.setInput("../samples/csv/data_sample_without_quotes.csv");
        encArgs.setOutput(output.toString());

        final long sourceLineCount;
        try (Stream<String> source = Files.lines(Paths.get(encArgs.getInput()), StandardCharsets.UTF_8)) {
            sourceLineCount = source.count();
        }

        final File outputFile = new File(encArgs.getOutput());
        outputFile.deleteOnExit();

        final int exitCode = EncryptMode.getApp(cleanRoomsDao).execute(encArgs.toArrayWithoutMode());
        assertEquals(0, exitCode);

        final String outputData = FileUtil.readBytes(output.toString());
        assertFalse(outputData.isBlank());

        final long targetLineCount;
        try (Stream<String> target = Files.lines(output, StandardCharsets.UTF_8)) {
            targetLineCount = target.count();
        }
        assertEquals(sourceLineCount, targetLineCount);

        // number of rows should be the same, regardless of how many columns we ended up generating
        final List<String[]> rows = CsvTestUtility.readContentAsArrays(encArgs.getOutput(), false);
        assertEquals(sourceLineCount, rows.size());

        // check each row in the result is the same size
        final int columnCount = rows.get(0).length;
        for (var row : rows) {
            assertEquals(row.length, columnCount);
        }

        // check the number of output columns is as expected
        assertEquals(11, columnCount);
    }

    // Check the decrypt command to make sure it works as expected
    @Test
    public void unmarshalTest() {
        decArgs.setInput("../samples/csv/marshalled_data_sample.csv");
        decArgs.setOutput(output.toString());

        final File outputFile = new File(decArgs.getOutput());

        final int exitCode = Main.getApp().execute(decArgs.toArray());
        assertEquals(0, exitCode);

        final String outputData = FileUtil.readBytes(outputFile.getAbsolutePath());
        assertFalse(outputData.isBlank());

        final List<String[]> preRows = CsvTestUtility.readContentAsArrays(decArgs.getInput(), false);
        final List<String[]> postRows = CsvTestUtility.readContentAsArrays(decArgs.getOutput(), false);
        // number of rows should have remained unchanged
        assertEquals(preRows.size(), postRows.size());

        // number of columns should be the same
        final int columnCount = preRows.get(0).length;
        assertEquals(columnCount, postRows.get(0).length);

        // number of columns for each row should match the columnCount
        for (int i = 0; i < preRows.size(); i++) {
            assertEquals(columnCount, preRows.get(i).length);
            assertEquals(columnCount, postRows.get(i).length);
        }
    }

    /*
     * Helper for basic round tripping tests - checks if the string will be interpreted as NULL
     * by the C3R client with default settings, so we can check this _or_ equality for correctness
     * depending on the input value.
     */
    private boolean defaultNullString(final String string) {
        return string.isBlank()
                || string.equals("\"\"");
    }

    /*
     * A "round trip" test that encrypts and then decrypts data, checking the values match or are still HMACed.
     *
     * The test takes an original input file and "explodes" it out, generating
     * 3 output columns for each input column such that each input column gets a
     * corresponding `cleartext` column, a `sealed` column, and a `fingerprint` column
     * as follows:
     * - Columns `[ColumnA, ...]` are transformed into
     * - columns `[ColumnA_cleartext, ColumnA_sealed, fingerprint, ...]` in the output.
     */
    public void clientRoundTripTest(final FileFormat fileFormat) throws IOException {
        // NOTE: We use a version of the sample data with enough quotes to make round trip
        // equalities work more simply
        final String originalPath;
        if (fileFormat == FileFormat.CSV) {
            originalPath = "../samples/csv/data_sample_with_quotes.csv";
        } else {
            originalPath = "../samples/parquet/data_sample.parquet";
        }
        final Path marshalledPath = FileTestUtility.resolve("clientRoundTripTest.marshalled." + fileFormat);
        final Path unmarshalledPath = FileTestUtility.resolve("clientRoundTripTest.unmarshalled." + fileFormat);
        final String marshalledStr = marshalledPath.toString();
        final String unmarshalledStr = unmarshalledPath.toString();

        encArgs.setInput(originalPath);
        encArgs.setOutput(marshalledStr);
        encArgs.setSchema("../samples/schema/config_sample_x3.json");
        encArgs.setPreserveNulls(false);

        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        int exitCode = EncryptMode.getApp(cleanRoomsDao).execute(encArgs.toArrayWithoutMode());
        assertEquals(0, exitCode);

        decArgs.setInput(marshalledStr);
        decArgs.setOutput(unmarshalledStr);
        decArgs.setFailOnFingerprintColumns(false);

        exitCode = Main.getApp().execute(decArgs.toArray());
        assertEquals(0, exitCode);

        final String outputData = FileUtil.readBytes(unmarshalledStr);
        assertFalse(outputData.isBlank());

        final List<String[]> preRows;
        final List<String[]> postRows;
        if (fileFormat == FileFormat.CSV) {
            preRows = CsvTestUtility.readContentAsArrays(originalPath, false);
            postRows = CsvTestUtility.readContentAsArrays(unmarshalledStr, false);
        } else {
            preRows = ParquetTestUtility.readContentAsStringArrays(originalPath);
            postRows = ParquetTestUtility.readContentAsStringArrays(unmarshalledStr);
        }

        // number of rows should be the same
        assertEquals(preRows.size(), postRows.size());

        // drop header row if source is a CSV file
        if (fileFormat == FileFormat.CSV) {
            preRows.remove(0);
            postRows.remove(0);
        }

        // IMPORTANT! The original data should have no duplicates in the first row,
        // so we can sort the data to easily compare it.
        preRows.sort(Comparator.comparing(row -> row[0]));
        postRows.sort(Comparator.comparing(row -> row[0]));

        // check that the cleartext and sealed columns returned
        // the same results back but the fingerprint column is still HMACed
        for (int i = 0; i < preRows.size(); i++) {
            final var preRow = preRows.get(i);
            final var postRow = postRows.get(i);
            assertEquals(preRow.length * 3, postRow.length);
            for (int j = 0; j < preRow.length; j++) {
                if (defaultNullString(preRow[j])) {
                    assertTrue(defaultNullString(postRow[j * 3]));
                    assertTrue(defaultNullString(postRow[j * 3 + 1]));
                } else {
                    assertEquals(preRow[j], postRow[j * 3]);
                    assertEquals(preRow[j], postRow[j * 3 + 1]);
                }
                assertNotEquals(preRow[j], postRow[j * 3 + 2]);
            }
        }

    }

    // Make sure non-interactive schema returns results
    @Test
    public void csvRoundTripTest() throws IOException {
        clientRoundTripTest(FileFormat.CSV);
    }

    @Test
    public void parquetRoundTripTest() throws IOException {
        clientRoundTripTest(FileFormat.PARQUET);
    }

}
