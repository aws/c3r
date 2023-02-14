// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.cli.CliTestUtility;
import com.amazonaws.c3r.cli.EncryptCliConfigTestUtility;
import com.amazonaws.c3r.cli.EncryptMode;
import com.amazonaws.c3r.cli.Main;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Test class for Positional Schema specific portions of JSON parsing.
public final class PositionalTableSchemaTypeAdapterTest implements TableSchemaCommonTypeAdapterTestInterface {
    // Positional cleartext column
    private static final KnownPartsToCompare CS_1 = new KnownPartsToCompare("c1", ColumnType.CLEARTEXT, null);

    // Pretty printed JSON string for {@code cs1}
    private static final String C_1 = writePrettyPrintJson(CS_1.target.toString(), CS_1.colType.toString(), CS_1.padType, CS_1.padLen);

    // Positional fingerprint column
    private static final KnownPartsToCompare CS_2 = new KnownPartsToCompare("c2", ColumnType.FINGERPRINT, null);

    // Pretty printed JSON string for {@code cs2}
    private static final String C_2 = writePrettyPrintJson(CS_2.target.toString(), CS_2.colType.toString(), CS_2.padType, CS_2.padLen);

    // Positional sealed column with no padding
    private static final KnownPartsToCompare CS_3 = new KnownPartsToCompare("c3", ColumnType.SEALED, Pad.DEFAULT);

    // Pretty printed JSON string for {@code cs3}
    private static final String C_3 = writePrettyPrintJson(CS_3.target.toString(), CS_3.colType.toString(), CS_3.padType, CS_3.padLen);

    // Positional sealed column with max padding of 100
    private static final KnownPartsToCompare CS_4 = new KnownPartsToCompare("c4", ColumnType.SEALED,
            Pad.builder().type(PadType.MAX).length(100).build());

    // Pretty printed JSON string for {@code cs4}
    private static final String C_4 = writePrettyPrintJson(CS_4.target.toString(), CS_4.colType.toString(), CS_4.padType, CS_4.padLen);

    // Positional sealed column with fixed padding of 50
    private static final KnownPartsToCompare CS_5 = new KnownPartsToCompare("c5", ColumnType.SEALED,
            Pad.builder().type(PadType.FIXED).length(50).build());

    // Pretty printed JSON string for {@code cs5}
    private static final String C_5 = writePrettyPrintJson(CS_5.target.toString(), CS_5.colType.toString(), CS_5.padType, CS_5.padLen);

    // Schema used for several I/O tests where a fixed value is useful
    private static final List<List<ColumnSchema>> SCHEMA_FOR_WRITE_TESTS = List.of(
            List.of(
                    ColumnSchema.builder().targetHeader(new ColumnHeader("c1")).type(ColumnType.CLEARTEXT).build(),
                    ColumnSchema.builder().targetHeader(new ColumnHeader("c2")).type(ColumnType.FINGERPRINT).build()
            ),
            List.of(
                    ColumnSchema.builder().targetHeader(new ColumnHeader("c3")).pad(Pad.DEFAULT).type(ColumnType.SEALED).build(),
                    ColumnSchema.builder().targetHeader(new ColumnHeader("c4"))
                            .pad(Pad.builder().type(PadType.MAX).length(100).build()).type(ColumnType.SEALED).build(),
                    ColumnSchema.builder().targetHeader(new ColumnHeader("c5"))
                            .pad(Pad.builder().type(PadType.FIXED).length(50).build()).type(ColumnType.SEALED).build()
            )
    );

    // Class to hold known values for a positional JSON string (we won't know source header until it's put in a specific column).
    private static class KnownPartsToCompare {
        private final ColumnHeader target;

        private final ColumnType colType;

        private final Pad pad;

        private final String padType;

        private final String padLen;

        // Create an instance with the given information.
        KnownPartsToCompare(final String target, final ColumnType type, final Pad pad) {
            this.target = new ColumnHeader(target);
            this.colType = type;
            this.pad = pad;
            if (pad != null) {
                padType = pad.getType().toString();
                if (pad.getType() != PadType.NONE) {
                    padLen = String.valueOf(pad.getLength());
                } else {
                    padLen = null;
                }
            } else {
                padType = null;
                padLen = null;
            }
        }
    }

    private Path schema;

    // Write out a pretty print formatted version of a positional column schema.
    private static String writePrettyPrintJson(final String targetHeader, final String type, final String padding, final String length) {
        final StringBuilder json = new StringBuilder();
        json.append("      {\n        \"type\": \"");
        json.append(type);
        json.append("\",\n");
        if (padding != null) {
            json.append("        \"pad\": {\n          \"type\": \"");
            json.append(padding);
            json.append("\"");
            if (length != null) {
                json.append(",\n          \"length\": ");
                json.append(length);
            }
            json.append("\n        },\n");
        }
        json.append("        \"targetHeader\": \"");
        json.append(targetHeader);
        json.append("\"\n      }");
        return json.toString();
    }

    // Construct a valid JSON string for a {@code PositionalTableSchema} including white space.
    private static String makeSchema(final String[][] specs) {
        final StringBuilder schema = new StringBuilder("{\n  \"columns\": [\n");
        for (int i = 0; i < specs.length; i++) {
            final var colSet = specs[i];
            if (colSet.length == 0) {
                schema.append("    []");
            } else {
                schema.append("    [\n");
                for (int j = 0; j < colSet.length; j++) {
                    schema.append(colSet[j]);
                    if (j < colSet.length - 1) {
                        schema.append(",\n");
                    } else {
                        schema.append("\n");
                    }
                }
                schema.append("    ]");
            }
            if (i < specs.length - 1) {
                schema.append(",\n");
            } else {
                schema.append("\n");
            }
        }
        schema.append("  ],\n  \"headerRow\": false\n}");
        return schema.toString();
    }

    // Take in a set of values and construct a column schema with the index as the source header.
    private static ColumnSchema makeColumnSchema(final int idx, final KnownPartsToCompare known) {
        return ColumnSchema.builder()
                .sourceHeader(ColumnHeader.getColumnHeaderFromIndex(idx))
                .targetHeader(known.target)
                .pad(known.pad)
                .type(known.colType)
                .build();
    }

    /* * * * BEGIN TESTS * * * */

    @BeforeEach
    public void setup() throws IOException {
        schema = FileTestUtility.createTempFile("schema", ".json");
    }

    // Parse a positional schema with rows that have no or multiple values and verify expected schema is read
    @Test
    public void complexPositionalSchemaTest() {
        final String json = "{ \"headerRow\": false, \"columns\": [" +
                "[{\"targetHeader\": \"column 1 cleartext\", \"type\": \"cleartext\"}, " +
                "{\"targetHeader\": \"column 1 sealed\", \"type\": \"sealed\", \"pad\": { \"type\": \"none\" } }," +
                "{\"targetHeader\": \"column 1 fingerprint\", \"type\": \"fingerprint\"}]," +
                "[]," +
                "[{\"targetHeader\": \"column 3\", \"type\": \"cleartext\"}]" +
                "]}";
        final TableSchema schema = GsonUtil.fromJson(json, TableSchema.class);
        final List<ColumnSchema> cols = schema.getColumns();
        assertEquals(4, cols.size());
        final ColumnSchema col1Cleartext = cols.get(0);
        final ColumnSchema col1Sealed = cols.get(1);
        final ColumnSchema col1Fingerprint = cols.get(2);
        assertEquals(ColumnHeader.getColumnHeaderFromIndex(0), col1Cleartext.getSourceHeader());
        assertEquals("column 1 cleartext", col1Cleartext.getTargetHeader().toString());
        assertEquals(ColumnType.CLEARTEXT, col1Cleartext.getType());
        assertEquals(ColumnHeader.getColumnHeaderFromIndex(0), col1Sealed.getSourceHeader());
        assertEquals("column 1 sealed", col1Sealed.getTargetHeader().toString());
        assertEquals(ColumnType.SEALED, col1Sealed.getType());
        assertEquals(Pad.DEFAULT, col1Sealed.getPad());
        assertEquals(ColumnHeader.getColumnHeaderFromIndex(0), col1Fingerprint.getSourceHeader());
        assertEquals("column 1 fingerprint", col1Fingerprint.getTargetHeader().toString());
        assertEquals(ColumnType.FINGERPRINT, col1Fingerprint.getType());
        final ColumnSchema col3 = cols.get(3);
        assertEquals(ColumnHeader.getColumnHeaderFromIndex(2), col3.getSourceHeader());
        assertEquals("column 3", col3.getTargetHeader().toString());
        assertEquals(ColumnType.CLEARTEXT, col3.getType());
    }

    /*
     * Add an extra column row to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooManyRowsPositionalSchemaTest() throws IOException {
        final String tempJson = FileUtil.readBytes("../samples/schema/config_sample_no_headers.json");
        final int closeOuter = tempJson.lastIndexOf("]");
        final String json = tempJson.substring(0, closeOuter - 1) + ", [] ] }";
        Files.writeString(schema, json);

        final EncryptCliConfigTestUtility args =
                EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv/data_sample_without_quotes.csv", schema.toString());
        args.setDryRun(false);

        final var inputArgs = args.toArrayWithoutMode();
        assertEquals(Main.FAILURE, EncryptMode.getApp(null).execute(inputArgs));
    }

    /*
     * Remove a column row to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooFewRowsPositionalSchemaTest() throws IOException {
        final String tempJson = FileUtil.readBytes("../samples/schema/config_sample_no_headers.json");
        final int lastElementStart = tempJson.lastIndexOf("],");
        final String json = tempJson.substring(0, lastElementStart - 1) + "]]}";
        Files.writeString(schema, json);

        final var args = EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv/data_sample_no_headers.csv", schema.toString());
        args.setDryRun(false);

        final var inputArgs = args.toArrayWithoutMode();
        assertEquals(Main.FAILURE, EncryptMode.getApp(null).execute(inputArgs));
    }

    /*
     * Make sure only the rows with ColumnSchemas are included in the output. Easiest to run through the CLI since we need
     * the CSV parser for verification.
     */
    @Test
    public void notAllRowsUsedTest() throws IOException {
        final String json = "{ \"headerRow\": false, \"columns\": [" +
                "[{\"targetHeader\":\"firstname\", \"type\": \"cleartext\"}]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]" +
                "]}";
        Files.writeString(schema, json);

        final EncryptCliConfigTestUtility args =
                EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv" + "/data_sample_without_quotes.csv", schema.toString());
        final String output = FileTestUtility.createTempFile().toString();
        args.setOutput(output);
        args.setDryRun(false);

        assertEquals(Main.SUCCESS, CliTestUtility.runWithoutCleanRooms(args));
        final List<Map<String, String>> rows = CsvTestUtility.readRows(args.getOutput());
        assertTrue(rows.size() > 0);
        for (Map<String, String> row : rows) {
            assertEquals(1, row.size());
            assertTrue(row.containsKey("firstname"));
        }
    }

    // Make sure null values in columns throw an error
    @Test
    public void verifyNullsRejectedTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> new PositionalTableSchema(List.of(
                        new ArrayList<>() {{
                            add(ColumnSchema.builder().targetHeader(new ColumnHeader("t1")).type(ColumnType.CLEARTEXT).build());
                            add(null);
                        }}
                )));
        assertEquals("Invalid empty column specification found for column 1", e.getMessage());
    }

    @Override
    @Test
    public void readSchemaAsTableSchemaWithValueValidationTest() {
        final String schema = makeSchema(new String[][]{{C_1, C_2}, {C_3}, {C_4, C_5}});
        final TableSchema table = GsonUtil.fromJson(schema, TableSchema.class);
        final List<ColumnSchema> csKnownValid = List.of(
                makeColumnSchema(0, CS_1),
                makeColumnSchema(0, CS_2),
                makeColumnSchema(1, CS_3),
                makeColumnSchema(2, CS_4),
                makeColumnSchema(2, CS_5)
        );
        assertEquals(csKnownValid, table.getColumns());
    }

    @Override
    @Test
    public void readSchemaAsActualClassWithValueValidationTest() {
        final String schema = makeSchema(new String[][]{{C_1}, {}, {C_2, C_3, C_4}, {C_5}});
        final PositionalTableSchema table = GsonUtil.fromJson(schema, PositionalTableSchema.class);
        final List<ColumnSchema> csKnownValid = List.of(
                makeColumnSchema(0, CS_1),
                makeColumnSchema(2, CS_2),
                makeColumnSchema(2, CS_3),
                makeColumnSchema(2, CS_4),
                makeColumnSchema(3, CS_5)
        );
        assertEquals(csKnownValid, table.getColumns());
    }

    // Verify that if we try to read a {@code PositionalTableSchema} into a {@code MappedTableSchema}, creation fails.
    @Override
    @Test
    public void readAsWrongSchemaClassTypeTest() {
        final String schema = makeSchema(new String[][]{{C_1}, {}, {C_2, C_3, C_4}, {C_5}});
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, MappedTableSchema.class));
    }

    @Override
    @Test
    public void readNullJsonInputTest() {
        assertNull(GsonUtil.fromJson("null", PositionalTableSchema.class));
    }

    @Override
    @Test
    public void readWithEmptyJsonClassTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("{}", PositionalTableSchema.class),
                "Schema was not initialized.");
    }

    @Override
    @Test
    public void readWithWrongJsonClassTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("true", PositionalTableSchema.class));
    }

    @Override
    @Test
    public void readWithWrongHeaderValueTest() {
        final String schema = "{\"headerRow\": true, columns:[[" + C_1 + ", " + C_2 + "], [" + C_3 + "]]}";
        // Interesting note: When called on the PositionalTableSchema class, GSON does not use TableSchemaTypeAdapter, so it's
        // validate() that catches the incorrect header type. When it's called on the TableSchema class it uses the adapter and
        // gets a syntax exception (correctly) instead.
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, PositionalTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
    }

    @Override
    @Test
    public void readWithJsonNullAsColumnsTest() {
        final String schema = "{headerRow: false, columns:null}";
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, PositionalTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
    }

    @Override
    @Test
    public void readWithWrongJsonTypeAsColumnsValueTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> GsonUtil.fromJson("{headerRow: false, columns:{0: [" + CS_1 + ", " + CS_2 + "]}}",
                        PositionalTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> GsonUtil.fromJson("{headerRow: false, columns:[{0: [" + CS_1 + ", " + CS_2 + "]}]}",
                        PositionalTableSchema.class));
    }

    @Override
    @Test
    public void verifyEmptyColumnsRejectedTest() {
        final String noColumnsSchema = "{headerRow: false, columns: []}";
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(noColumnsSchema, PositionalTableSchema.class));
        final String emptyColumnsSchema = "headerRow: false, columns:[[],[],[]]}";
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(emptyColumnsSchema, PositionalTableSchema.class));
    }

    @Override
    @Test
    public void writeSchemaAsTableSchemaTest() {
        final TableSchema table = new PositionalTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final String schemaWithClass = GsonUtil.toJson(table, TableSchema.class);
        final String schemaWithoutClass = GsonUtil.toJson(table);
        final String schemaActual = makeSchema(new String[][]{{C_1, C_2}, {C_3, C_4, C_5}});
        assertEquals(schemaActual, schemaWithClass);
        assertEquals(schemaActual, schemaWithoutClass);
    }

    @Override
    @Test
    public void writeSchemaAsActualClassTest() {
        final PositionalTableSchema table = new PositionalTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final String schemaWithClass = GsonUtil.toJson(table, PositionalTableSchema.class);
        final String schemaWithoutClass = GsonUtil.toJson(table);
        final String schemaActual = makeSchema(new String[][]{{C_1, C_2}, {C_3, C_4, C_5}});
        assertEquals(schemaActual, schemaWithClass);
        assertEquals(schemaActual, schemaWithoutClass);
    }

    /*
     * Verify that writing {@code PositionalTableSchema} to JSON fails when {@code MappedTableSchema} is specified,
     * regardless of whether the object is referred to as a {@code TableSchema} or {@code PositionalTableSchema}
     */
    @Override
    @Test
    public void writeSchemaAsWrongClassTest() {
        final TableSchema table = new PositionalTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final PositionalTableSchema positionalTable = new PositionalTableSchema(SCHEMA_FOR_WRITE_TESTS);
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.toJson(table, MappedTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.toJson(positionalTable, MappedTableSchema.class));
    }

    /*
     * Verify that the child class writes the {@code null} value out to disk correctly when
     * - called as {@code TableSchema} or
     * - called as {@code MappedTableSchema}
     */
    @Override
    @Test
    public void writeNullJsonInputTest() {
        assertEquals("null", GsonUtil.toJson(null, TableSchema.class));
        assertEquals("null", GsonUtil.toJson(null, PositionalTableSchema.class));
    }

    /*
     * Starting with a JSON string, verify the same JSON string is returned when
     * - Reading it in as a {@code TableSchema} and writing it out as a {@code TableSchema}
     * - Reading it in as a {@code PositionalTableSchema} and writing it out as a {@code TableSchema}
     * - Reading it in as a {@code TableSchema} and writing it out as a {@code PositionalTableSchema}
     * - Reading it in as a {@code PositionalTableSchema} and writing it out as a {@code PositionalTableSchema}
     */
    @Override
    @Test
    public void roundTripStringToStringSerializationTest() {
        final String schemaIn = makeSchema(new String[][]{{C_1, C_2}, {}, {C_3}, {C_4}, {C_5}, {}});
        final String tableSchemaInTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, TableSchema.class), TableSchema.class);
        assertEquals(schemaIn, tableSchemaInTableSchemaOut);
        final String positionalSchemaInTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, PositionalTableSchema.class),
                TableSchema.class);
        assertEquals(schemaIn, positionalSchemaInTableSchemaOut);
        final String tableSchemaInPositionalSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, TableSchema.class),
                PositionalTableSchema.class);
        assertEquals(schemaIn, tableSchemaInPositionalSchemaOut);
        final String positionalSchemaInPositionalSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, PositionalTableSchema.class),
                PositionalTableSchema.class);
        assertEquals(schemaIn, positionalSchemaInPositionalSchemaOut);
    }

    /*
     * Starting with an instance of a PositionalTableSchema class, verify the same value is returned when
     * - Writing it out as a {@code TableSchema} and reading it in as a {@code TableSchema}
     * - Writing it out as a {@code PositionalTableSchema} and reading it in as a {@code TableSchema}
     * - Writing it out as a {@code TableSchema} and reading it in as a {@code PositionalTableSchema}
     * - Writing it out as a {@code PositionalTableSchema} and reading it in as a {@code PositionalTableSchema}
     */
    @Override
    @Test
    public void roundTripClassToClassSerializationTest() {
        final TableSchema tableIn = new PositionalTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final TableSchema tableSchemaInTableSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, TableSchema.class),
                TableSchema.class);
        assertEquals(tableIn, tableSchemaInTableSchemaOut);
        final TableSchema positionalSchemaInTableSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, PositionalTableSchema.class),
                TableSchema.class);
        assertEquals(tableIn, positionalSchemaInTableSchemaOut);
        final PositionalTableSchema tableSchemaInPositionalSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, TableSchema.class),
                PositionalTableSchema.class);
        assertEquals(tableIn, tableSchemaInPositionalSchemaOut);
        final PositionalTableSchema positionalSchemaInPositionalSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn,
                PositionalTableSchema.class), PositionalTableSchema.class);
        assertEquals(tableIn, positionalSchemaInPositionalSchemaOut);
    }
}
