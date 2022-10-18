// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InteractiveSchemaGeneratorTest {
    private final List<ColumnHeader> headers = Stream.of(
                    "header1",
                    "header2",
                    "header3"
            ).map(ColumnHeader::new)
            .collect(Collectors.toList());

    private final List<ClientDataType> stringColumnTypes = Collections.nCopies(headers.size(), ClientDataType.STRING);

    private final List<ClientDataType> unknownColumnTypes = Collections.nCopies(headers.size(), ClientDataType.UNKNOWN);

    private final String exampleMappedSchemaString =
            String.join("\n",
                    "{",
                    "  \"headerRow\": true,",
                    "  \"columns\": [",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2_sealed\",",
                    "      \"type\": \"sealed\",",
                    "      \"pad\": {",
                    "        \"type\": \"NONE\"",
                    "      }",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2_fingerprint\",",
                    "      \"type\": \"fingerprint\"",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2\",",
                    "      \"type\": \"cleartext\"",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header3\",",
                    "      \"targetHeader\": \"header3\",",
                    "      \"type\": \"sealed\",",
                    "      \"pad\": {",
                    "        \"type\": \"MAX\",",
                    "        \"length\": \"0\"",
                    "      }",
                    "    }",
                    "  ]",
                    "}");

    private final String examplePositionalSchemaString =
            String.join("\n",
                    "{",
                    "  \"headerRow\": false,",
                    "  \"columns\": [",
                    "    [],",
                    "    [",
                    "      {",
                    "        \"type\": \"sealed\",",
                    "        \"pad\": {",
                    "          \"type\": \"NONE\"",
                    "        },",
                    "        \"targetHeader\": \"targetheader2_sealed\"",
                    "      },",
                    "      {",
                    "        \"type\": \"fingerprint\",",
                    "        \"targetHeader\": \"targetheader2_fingerprint\"",
                    "      },",
                    "      {",
                    "        \"type\": \"cleartext\",",
                    "        \"targetHeader\": \"targetheader2\"",
                    "      }",
                    "    ],",
                    "    [",
                    "      {",
                    "        \"type\": \"sealed\",",
                    "        \"pad\": {",
                    "          \"type\": \"MAX\",",
                    "          \"length\": 0",
                    "        },",
                    "        \"targetHeader\": \"targetheader3\"",
                    "      }",
                    "    ]",
                    "  ]",
                    "}");

    private final String exampleMappedSchemaAllCleartextString =
            String.join("\n",
                    "{",
                    "  \"headerRow\": true,",
                    "  \"columns\": [",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2_1\",",
                    "      \"type\": \"cleartext\"",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2_2\",",
                    "      \"type\": \"cleartext\"",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header2\",",
                    "      \"targetHeader\": \"targetheader2_3\",",
                    "      \"type\": \"cleartext\"",
                    "    },",
                    "    {",
                    "      \"sourceHeader\": \"header3\",",
                    "      \"targetHeader\": \"header3\",",
                    "      \"type\": \"cleartext\"",
                    "    }",
                    "  ]",
                    "}");

    private final String examplePositionalSchemaAllCleartextString =
            String.join("\n",
                    "{",
                    "  \"headerRow\": false,",
                    "  \"columns\": [",
                    "    [],",
                    "    [",
                    "      {",
                    "        \"type\": \"cleartext\",",
                    "        \"targetHeader\": \"targetheader2_1\"",
                    "      },",
                    "      {",
                    "        \"type\": \"cleartext\",",
                    "        \"targetHeader\": \"targetheader2_2\"",
                    "      },",
                    "      {",
                    "        \"type\": \"cleartext\",",
                    "        \"targetHeader\": \"targetheader2_3\"",
                    "      }",
                    "    ],",
                    "    [",
                    "      {",
                    "        \"type\": \"cleartext\",",
                    "        \"targetHeader\": \"targetheader3\"",
                    "      }",
                    "    ]",
                    "  ]",
                    "}");

    private InteractiveSchemaGenerator schemaGen;

    private Path targetSchema;

    private ByteArrayOutputStream consoleOutput;

    // Set up the interactive generator.
    private void setup(final String simulatedUserInput, final List<ColumnHeader> headers, final List<ClientDataType> types)
            throws IOException {
        final Path tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        targetSchema = tempDir.resolve("schema.json");
        targetSchema.toFile().deleteOnExit();
        final var userInput = new BufferedReader(new StringReader(simulatedUserInput + "\n"));
        consoleOutput = new ByteArrayOutputStream();
        schemaGen = InteractiveSchemaGenerator.builder()
                .sourceHeaders(headers)
                .sourceColumnTypes(types)
                .targetJsonFile(targetSchema.toAbsolutePath().toString())
                .consoleInput(userInput)
                .consoleOutput(new PrintStream(consoleOutput, true, StandardCharsets.UTF_8))
                .build();
    }

    @Test
    public void validateErrorWithMismatchedColumnCounts() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                setup("", headers, List.of()));
    }

    @Test
    public void promptNonnegativeIntValidTest() throws IOException {
        final List<String> validInputs = List.of("42", "0", "100");
        for (var input : validInputs) {
            setup(input, headers, stringColumnTypes);
            assertEquals(
                    Integer.valueOf(input),
                    schemaGen.promptNonNegativeInt("", null, 100));
            assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        }
    }

    @Test
    public void promptNonnegativeIntInvalidTest() throws IOException {
        final List<String> validInputs = List.of("", "NotANumber", "-1", "101");
        for (var input : validInputs) {
            setup(input, headers, stringColumnTypes);
            assertNull(schemaGen.promptNonNegativeInt("", null, 100));
            assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
        }
    }

    @Test
    public void promptNonNegativeIntValidDefaultTest() throws IOException {
        final List<String> validInputs = List.of("1", "", "3");
        for (var input : validInputs) {
            setup(input, headers, stringColumnTypes);
            assertEquals(
                    input.isBlank() ? 2 : Integer.parseInt(input),
                    schemaGen.promptNonNegativeInt("", 2, 100));
            assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        }
    }

    @Test
    public void promptYesOrNoValidTest() throws IOException {
        final List<Boolean> defaultBooleanAnswers = Arrays.asList(null, true, false);
        final List<String> validYesStrings = List.of("y", "yes", "Y", "YES");

        for (var input : validYesStrings) {
            for (var answer : defaultBooleanAnswers) {
                setup(input, headers, stringColumnTypes);
                assertTrue(schemaGen.promptYesOrNo("", answer));
                assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
            }
        }

        final List<String> validNoStrings = List.of("n", "no", "N", "NO");
        for (var input : validNoStrings) {
            for (var answer : defaultBooleanAnswers) {
                setup(input, headers, stringColumnTypes);
                assertFalse(schemaGen.promptYesOrNo("", answer));
                assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
            }
        }

        for (var answer : defaultBooleanAnswers) {
            setup("", headers, stringColumnTypes);
            assertEquals(answer, schemaGen.promptYesOrNo("", answer));
            if (answer == null) {
                assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
            } else {
                assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
            }
        }
    }

    @Test
    public void promptYesOrNoInvalidTest() throws IOException {
        setup("", headers, stringColumnTypes);
        assertNull(schemaGen.promptYesOrNo("", null));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("ja", headers, stringColumnTypes);
        assertNull(schemaGen.promptYesOrNo("", null));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("nein", headers, stringColumnTypes);
        assertNull(schemaGen.promptYesOrNo("", null));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptColumnTypeValidTest() throws IOException {
        final List<String> validCleartextInputs = List.of("c", "C", "cleartext", "CLEARTEXT");
        for (var input : validCleartextInputs) {
            setup(input, headers, stringColumnTypes);
            assertEquals(ColumnType.CLEARTEXT, schemaGen.promptColumnType());
            assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        }

        final List<String> validFingerprintInputs = List.of("f", "F", "fingerprint", "FINGERPRINT");
        for (var input : validFingerprintInputs) {
            setup(input, headers, stringColumnTypes);
            assertEquals(ColumnType.FINGERPRINT, schemaGen.promptColumnType());
            assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        }

        final List<String> validSealedInputs = List.of("s", "S", "sealed", "SEALED");
        for (var input : validSealedInputs) {
            setup(input, headers, stringColumnTypes);
            assertEquals(ColumnType.SEALED, schemaGen.promptColumnType());
            assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        }
    }

    @Test
    public void promptColumnTypeInvalidTest() throws IOException {
        final List<String> validCleartextInputs = List.of("", "a", "unrostricted", "solekt", "joyn");
        for (var input : validCleartextInputs) {
            setup(input, headers, stringColumnTypes);
            assertNull(schemaGen.promptColumnType());
            assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
        }
    }

    @Test
    public void promptTargetHeaderSuffixTest() throws IOException {
        setup("", headers, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeaderSuffix(ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("y", headers, stringColumnTypes);
        assertEquals("_sealed", schemaGen.promptTargetHeaderSuffix(ColumnType.SEALED));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("n", headers, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeaderSuffix(ColumnType.SEALED));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("", headers, stringColumnTypes);
        assertEquals("_fingerprint", schemaGen.promptTargetHeaderSuffix(ColumnType.FINGERPRINT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("n", headers, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeaderSuffix(ColumnType.FINGERPRINT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptTargetHeaderTest() throws IOException {
        setup("", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("a"), schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("b", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("b"), schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        assertFalse(consoleOutput.toString().toLowerCase().contains("normalized"));

        setup("B", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("b"), schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        assertTrue(consoleOutput.toString().toLowerCase().contains("normalized"));

        setup("b".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH) + 1, headers, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.CLEARTEXT));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptTargetHeaderWithoutSourceHeadersTest() throws IOException {
        // empty input does _not_ give you a default target header when no source headers exist
        setup("", null, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeader(null, ColumnType.CLEARTEXT));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));

        // providing input for a target header when source headers are null remains unchanged
        setup("b", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("b"), schemaGen.promptTargetHeader(null, ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("B", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("b"), schemaGen.promptTargetHeader(null, ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
        assertTrue(consoleOutput.toString().toLowerCase().contains("normalized"));
    }

    @Test
    public void promptTargetHeaderAlreadyUsedHeaderTest() throws IOException {
        setup("\n", headers, stringColumnTypes);
        assertEquals(new ColumnHeader("header"), schemaGen.promptTargetHeader(new ColumnHeader("header"), ColumnType.CLEARTEXT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        assertNull(schemaGen.promptTargetHeader(new ColumnHeader("header"), ColumnType.CLEARTEXT));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptTargetHeaderWithSuffixTest() throws IOException {
        final String suffix = ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX;
        setup("\n", headers, stringColumnTypes);
        assertEquals(
                new ColumnHeader("a_fingerprint"),
                schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.FINGERPRINT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("b".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH - suffix.length())
                + "\n", headers, stringColumnTypes);
        assertEquals(
                new ColumnHeader(
                        "b".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH - suffix.length())
                                + suffix),
                schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.FINGERPRINT));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptTargetHeaderCannotAddSuffixTest() throws IOException {
        setup("a".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH)
                + "\n", headers, stringColumnTypes);
        assertNull(schemaGen.promptTargetHeader(new ColumnHeader("a"), ColumnType.FINGERPRINT));
        assertTrue(consoleOutput.toString().toLowerCase().contains("unable to add header suffix"));
    }

    @Test
    public void promptPadTypeTest() throws IOException {
        final var header = new ColumnHeader("a");
        final PadType nullDefaultType = null;
        setup("", headers, stringColumnTypes);
        assertNull(schemaGen.promptPadType(header, nullDefaultType));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("", headers, stringColumnTypes);
        assertEquals(PadType.MAX, schemaGen.promptPadType(header, PadType.MAX));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("n", headers, stringColumnTypes);
        assertEquals(PadType.NONE, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("none", headers, stringColumnTypes);
        assertEquals(PadType.NONE, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("f", headers, stringColumnTypes);
        assertEquals(PadType.FIXED, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("fixed", headers, stringColumnTypes);
        assertEquals(PadType.FIXED, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("m", headers, stringColumnTypes);
        assertEquals(PadType.MAX, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("max", headers, stringColumnTypes);
        assertEquals(PadType.MAX, schemaGen.promptPadType(header, nullDefaultType));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("unknown", headers, stringColumnTypes);
        assertNull(schemaGen.promptPadType(header, nullDefaultType));
        assertTrue(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptPadTest() throws IOException {
        final var header = new ColumnHeader("a");
        setup("n", headers, stringColumnTypes);
        assertEquals(
                Pad.DEFAULT,
                schemaGen.promptPad(header));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("f\n42", headers, stringColumnTypes);
        assertEquals(
                Pad.builder().type(PadType.FIXED).length(42).build(),
                schemaGen.promptPad(header));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));

        setup("m\n42", headers, stringColumnTypes);
        assertEquals(
                Pad.builder().type(PadType.MAX).length(42).build(),
                schemaGen.promptPad(header));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptColumnInfoWithSourceHeadersTest() throws IOException {
        final String columnType = "sealed";
        final String targetName = "target";
        final String useSuffix = "no";
        final String paddingType = "none";
        setup(String.join("\n",
                        columnType,
                        targetName,
                        useSuffix,
                        paddingType),
                headers,
                stringColumnTypes);
        assertEquals(
                ColumnSchema.builder()
                        .sourceHeader(new ColumnHeader("source"))
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.SEALED)
                        .pad(Pad.DEFAULT)
                        .build(),
                schemaGen.promptColumnInfo(new ColumnHeader("source"), 1, 2));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptColumnInfoWithSourceHeadersAndUnknownTypeTest() throws IOException {
        setup("target", headers, unknownColumnTypes);
        assertEquals(
                ColumnSchema.builder()
                        .sourceHeader(new ColumnHeader("source"))
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.CLEARTEXT)
                        .build(),
                schemaGen.promptColumnInfo(new ColumnHeader("source"), 1, 2));
        assertTrue(consoleOutput.toString().toLowerCase().contains("cryptographic computing is not supported"));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptColumnInfoWithoutSourceHeadersTest() throws IOException {
        setup("", null, stringColumnTypes);
        final String columnType = "sealed";
        final String targetName = "target";
        final String useSuffix = "no";
        final String paddingType = "none";
        setup(String.join("\n",
                        columnType,
                        targetName,
                        useSuffix,
                        paddingType),
                headers,
                stringColumnTypes);
        assertEquals(
                ColumnSchema.builder()
                        .sourceHeader(null)
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.SEALED)
                        .pad(Pad.builder().type(PadType.NONE).build())
                        .build(),
                schemaGen.promptColumnInfo(null, 1, 2));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void promptColumnInfoWithoutSourceHeadersAndUnknownTypeTest() throws IOException {
        setup("target", null, unknownColumnTypes);
        assertEquals(
                ColumnSchema.builder()
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.CLEARTEXT)
                        .build(),
                schemaGen.promptColumnInfo(null, 1, 2));
        assertTrue(consoleOutput.toString().toLowerCase().contains("cryptographic computing is not supported"));
        assertFalse(consoleOutput.toString().toLowerCase().contains("expected"));
    }

    @Test
    public void runGenerateNoSchemaTest() throws IOException {
        // 0 target columns to generate for each source column
        setup("0\n".repeat(headers.size()), headers, stringColumnTypes);
        schemaGen.run();
        assertTrue(consoleOutput.toString().contains("No target columns were specified."));
        assertEquals(0, targetSchema.toFile().length());
    }

    @Test
    public void runGenerateSchemaWithSourceHeadersTest() throws IOException {
        final String userInput =
                String.join("\n",
                        // source header1
                        "0", // number of columns for header1
                        // source header2
                        "3", // number of columns for header2
                        // header2, column 1
                        "sealed", // header2, column 1 type
                        "targetHeader2", // header2, column 1 target header
                        "yes", // header2, column 1 use suffix
                        "none", // header2, column 1 padding type
                        // header2, column 2
                        "fingerprint", // header2, column 2 type
                        "targetHeader2", // header2, column 2 target header
                        "yes", // header2, column 2 use suffix
                        // header2, column 3
                        "cleartext", // header2, column 3 type
                        "targetHeader2", // header2, column 3 target header
                        // source header3
                        "", // number of columns for header3 (default to 1)
                        "sealed",
                        "", // header3, column 1 target header (default)
                        "n", // header3, column 1 use suffix
                        "max", // header3, column 1 padding type
                        "" // header3, column 1 padding length (default 0)
                );
        setup(userInput, headers, stringColumnTypes);
        schemaGen.run();
        assertNotEquals(0, targetSchema.toFile().length());

        final var expectedSchema = GsonUtil.fromJson(exampleMappedSchemaString, TableSchema.class);
        final var actualSchema = GsonUtil.fromJson(FileUtil.readBytes(targetSchema.toAbsolutePath().toString()), TableSchema.class);
        assertEquals(GsonUtil.toJson(expectedSchema), GsonUtil.toJson((actualSchema)));
    }

    @Test
    public void runGenerateSchemaWithSourceHeadersUnknownTypesTest() throws IOException {
        final String userInput =
                String.join("\n",
                        // source header1
                        "0", // number of columns for header1
                        // source header2
                        "3", // number of columns for header2
                        // header2, column 1
                        // type is cleartext due to unknown client type
                        "targetHeader2_1", // header2, column 1 target header
                        // header2, column 2
                        // type is cleartext due to unknown client type
                        "targetHeader2_2", // header2, column 2 target header
                        // header2, column 3
                        // type is cleartext due to unknown client type
                        "targetHeader2_3", // header2, column 2 target header
                        // source header3
                        "", // number of columns for header3 (default to 1)
                        // type is cleartext due to unknown client type
                        "" // header3, column 1 target header (default)
                );
        setup(userInput, headers, unknownColumnTypes);
        schemaGen.run();
        assertNotEquals(0, targetSchema.toFile().length());
        System.err.println("Expected: " + exampleMappedSchemaAllCleartextString);

        final var expectedSchema = GsonUtil.fromJson(exampleMappedSchemaAllCleartextString, TableSchema.class);
        final var actualSchema = GsonUtil.fromJson(FileUtil.readBytes(targetSchema.toAbsolutePath().toString()), TableSchema.class);
        assertEquals(GsonUtil.toJson(expectedSchema), GsonUtil.toJson((actualSchema)));
    }

    @Test
    public void runGenerateSchemaWithoutSourceHeadersTest() throws IOException {
        final String userInput =
                String.join("\n",
                        // source header1
                        "0", // number of columns for header1
                        // source header2
                        "3", // number of columns for header2
                        // header2, column 1
                        "sealed", // header2, column 1 type
                        "targetHeader2", // header2, column 1 target header
                        "yes", // header2, column 1 use suffix
                        "none", // header2, column 1 padding type
                        // header2, column 2
                        "fingerprint", // header2, column 2 type
                        "targetHeader2", // header2, column 2 target header
                        "yes", // header2, column 2 use suffix
                        // header2, column 3
                        "cleartext", // header2, column 3 type
                        "targetHeader2", // header2, column 3 target header
                        // source header3
                        "", // number of columns for header3 (default to 1)
                        "sealed",
                        "targetHeader3", // header3, column 1 target header (default)
                        "n", // header3, column 1 use suffix
                        "max", // header3, column 1 padding type
                        "" // header3, column 1 padding length (default 0)
                );
        setup(userInput, null, stringColumnTypes);
        schemaGen.run();
        assertNotEquals(0, targetSchema.toFile().length());

        final var expectedSchema = GsonUtil.fromJson(examplePositionalSchemaString, TableSchema.class);
        final var actualSchema = GsonUtil.fromJson(FileUtil.readBytes(targetSchema.toAbsolutePath().toString()), TableSchema.class);
        assertEquals(GsonUtil.toJson(expectedSchema), GsonUtil.toJson((actualSchema)));
    }

    @Test
    public void runGenerateSchemaWithoutSourceHeadersUnknownTypesTest() throws IOException {
        final String userInput =
                String.join("\n",
                        // source header1
                        "0", // number of columns for header1
                        // source header2
                        "3", // number of columns for header2
                        // header2, column 1
                        // type is cleartext due to unknown client type
                        "targetHeader2_1", // header2, column 1 target header
                        // header2, column 2
                        // type is cleartext due to unknown client type
                        "targetHeader2_2", // header2, column 2 target header
                        // header2, column 3
                        // type is cleartext due to unknown client type
                        "targetHeader2_3", // header2, column 3 target header
                        // source header3
                        "", // number of columns for header3 (default to 1)
                        // type is cleartext due to unknown client type
                        "targetHeader3" // header3, column 1 target header (default)
                );
        setup(userInput, null, unknownColumnTypes);
        schemaGen.run();
        assertNotEquals(0, targetSchema.toFile().length());

        final var expectedSchema = GsonUtil.fromJson(examplePositionalSchemaAllCleartextString, TableSchema.class);
        final var actualSchema = GsonUtil.fromJson(FileUtil.readBytes(targetSchema.toAbsolutePath().toString()), TableSchema.class);
        assertEquals(GsonUtil.toJson(expectedSchema), GsonUtil.toJson((actualSchema)));
    }

    @Test
    public void runTestWithBadInputsMixedIn() throws IOException {
        final String userInput =
                String.join("\n",
                        // source header1
                        "zero", // bad number of columns for header1
                        "0", // number of columns for header1
                        // source header2
                        "three", // bad number of columns
                        "3", // number of columns
                        // header 2, column 1
                        "special", // bad column type
                        "sealed", // header 2, column 1 type
                        "long_name".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH), // header 2, column 1 bad target header
                        "targetHeader2", // header 2, column 1 target header
                        "maybe", // header 2, column 1 bad use suffix
                        "yes", // header 2, column 1 use suffix
                        "super", // header 2, column 1 bad padding type
                        "none", // header 2, column 1 padding type
                        // header 2, column 2
                        "goin", // header 2, column 2 bad type
                        "fingerprint", // header 2, column 2 type
                        "long_name".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH), // header 2, column 2 bad target header
                        "targetHeader2", // header 2, column 2 target header
                        "I can't decide", // header 2, column 2 bad use suffix
                        "yes", // header 2, column 2 use suffix
                        // header 2, column 3
                        "plaintext", // header 2, column 3 bad type
                        "cleartext", // header 2, column 3 type
                        "long_name".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH), // header 2, column 3 bad target header
                        "targetHeader2", // header 2, column 3 target header
                        // source header3
                        "one", // bad number of columns for header3
                        "", // number of columns for header3 (default to 1)
                        "sealed",
                        "", // header3, column 1 target header (default)
                        "what", // bad header3, column 1 use suffix
                        "n", // header3, column 1 use suffix
                        "mux", // bad header3, column 1 padding type
                        "max", // header3, column 1 padding type
                        "zero", // header3, column 1 padding length (default 0)
                        "" // header3, column 1 padding length (default 0)
                );
        setup(userInput, headers, stringColumnTypes);
        schemaGen.run();
        assertNotEquals(0, targetSchema.toFile().length());

        final TableSchema expectedSchema = GsonUtil.fromJson(exampleMappedSchemaString, TableSchema.class);
        final TableSchema actualSchema = GsonUtil.fromJson(FileUtil.readBytes(targetSchema.toAbsolutePath().toString()), TableSchema.class);
        assertEquals(GsonUtil.toJson(expectedSchema), GsonUtil.toJson(actualSchema));
    }

    @Test
    public void nullValueCsvSchemaGeneratorTest() {
        // no headers
        assertThrows(NullPointerException.class, () -> CsvSchemaGenerator.builder()
                .inputCsvFile("../samples/csv/data_sample_without_quotes.csv")
                .targetJsonFile(targetSchema.toFile().getAbsolutePath())
                .overwrite(true).build());
        // no target
        assertThrows(NullPointerException.class, () -> CsvSchemaGenerator.builder()
                .inputCsvFile("../samples/csv/data_sample_without_quotes.csv")
                .overwrite(true)
                .hasHeaders(true).build());
        // no input
        assertThrows(NullPointerException.class,
                () -> CsvSchemaGenerator.builder()
                        .targetJsonFile(targetSchema.toFile().getAbsolutePath())
                        .overwrite(true)
                        .hasHeaders(true).build());
        // no overwrite
        assertThrows(NullPointerException.class, () -> CsvSchemaGenerator.builder()
                .inputCsvFile("../samples/csv/data_sample_without_quotes.csv")
                .targetJsonFile(targetSchema.toFile().getAbsolutePath())
                .hasHeaders(true).build());
    }
}
