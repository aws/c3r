// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

// Test class for Mapped Schema specific portions of JSON parsing
public class MappedTableSchemaTypeAdapterTest implements TableSchemaCommonTypeAdapterTestInterface {
    // {@code ColumnSchema} cleartext
    private static final ColumnSchema CS_1 =
            ColumnSchema.builder().sourceHeader(new ColumnHeader("source1")).targetHeader(new ColumnHeader("c1"))
                    .type(ColumnType.CLEARTEXT).build();

    // Pretty printed JSON string for {@code cs1}
    private static final String C_1 = writePrettyPrintJson(CS_1.getSourceHeader().toString(), CS_1.getTargetHeader().toString(),
            CS_1.getType().toString(), getPadType(CS_1), getPadLen(CS_1));

    // {@code ColumnSchema} fingerprint
    private static final ColumnSchema CS_2 =
            ColumnSchema.builder().sourceHeader(new ColumnHeader("source2")).targetHeader(new ColumnHeader("c2"))
                    .type(ColumnType.FINGERPRINT).build();

    // Pretty printed JSON string for {@code cs2}
    private static final String C_2 = writePrettyPrintJson(CS_2.getSourceHeader().toString(), CS_2.getTargetHeader().toString(),
            CS_2.getType().toString(), getPadType(CS_2), getPadLen(CS_2));

    // {@code ColumnSchema} sealed with no padding
    private static final ColumnSchema CS_3 =
            ColumnSchema.builder().sourceHeader(new ColumnHeader("source3")).targetHeader(new ColumnHeader("c3"))
                    .pad(Pad.DEFAULT).type(ColumnType.SEALED).build();

    // Pretty printed JSON string for {@code cs3}
    private static final String C_3 = writePrettyPrintJson(CS_3.getSourceHeader().toString(), CS_3.getTargetHeader().toString(),
            CS_3.getType().toString(), getPadType(CS_3), getPadLen(CS_3));

    // {@code ColumnSchema} sealed with max pad of 100
    private static final ColumnSchema CS_4 =
            ColumnSchema.builder().sourceHeader(new ColumnHeader("source4")).targetHeader(new ColumnHeader("c4"))
                    .pad(Pad.builder().type(PadType.MAX).length(100).build()).type(ColumnType.SEALED).build();

    // Pretty printed JSON string for {@code cs4}
    private static final String C_4 = writePrettyPrintJson(CS_4.getSourceHeader().toString(), CS_4.getTargetHeader().toString(),
            CS_4.getType().toString(), getPadType(CS_4), getPadLen(CS_4));

    // {@code ColumnSchema} sealed with fixed pad of 50
    private static final ColumnSchema CS_5 =
            ColumnSchema.builder().sourceHeader(new ColumnHeader("source5")).targetHeader(new ColumnHeader("c5"))
                    .pad(Pad.builder().type(PadType.FIXED).length(50).build()).type(ColumnType.SEALED).build();

    // Pretty printed JSON string for {@code cs5}
    private static final String C_5 = writePrettyPrintJson(CS_5.getSourceHeader().toString(), CS_5.getTargetHeader().toString(),
            CS_5.getType().toString(), getPadType(CS_5), getPadLen(CS_5));

    // Schema used for several I/O tests where a fixed value is useful
    private static final List<ColumnSchema> SCHEMA_FOR_WRITE_TESTS = List.of(CS_1, CS_2, CS_3, CS_4, CS_5);

    /*
     * Helper function that creates the pretty printed format (correct white space) for a positional column schema for when we do
     * comparison of json outputs.
     */
    private static String writePrettyPrintJson(final String sourceHeader, final String targetHeader, final String colType,
                                               final String padType, final String padLen) {
        final StringBuilder json = new StringBuilder();
        json.append("    {\n      \"type\": \"");
        json.append(colType);
        json.append("\",\n");
        if (padType != null) {
            json.append("      \"pad\": {\n        \"type\": \"");
            json.append(padType);
            json.append("\"");
            if (padLen != null) {
                json.append(",\n        \"length\": ");
                json.append(padLen);
            }
            json.append("\n      },\n");
        }
        json.append("      \"sourceHeader\": \"");
        json.append(sourceHeader);
        json.append("\",\n      \"targetHeader\": \"");
        json.append(targetHeader);
        json.append("\"\n    }");
        return json.toString();
    }

    // Helper function for getting the pad type if any.
    private static String getPadType(final ColumnSchema cs) {
        final Pad p = cs.getPad();
        if (p == null) {
            return null;
        }
        return p.getType().toString();
    }

    // Helper function for getting the pad length if any.
    private static String getPadLen(final ColumnSchema cs) {
        final Pad p = cs.getPad();
        if (p == null) {
            return null;
        }
        final Integer len = p.getLength();
        return (len == null) ? null : String.valueOf(len);
    }

    // Construct a valid JSON string for a {@code MappedTableSchema} including white space.
    private static String makeSchema(final String[] specs) {
        final StringBuilder schema = new StringBuilder("{\n  \"columns\": [\n");
        for (int i = 0; i < specs.length; i++) {
            schema.append(specs[i]);
            if (i < specs.length - 1) {
                schema.append(",\n");
            } else {
                schema.append("\n");
            }
        }
        schema.append("  ],\n  \"headerRow\": true\n}");
        return schema.toString();
    }

    /* * * * BEGIN TESTS * * * */

    @Override
    @Test
    public void readSchemaAsTableSchemaWithValueValidationTest() {
        final String schema = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        final TableSchema table = GsonUtil.fromJson(schema, TableSchema.class);
        final List<ColumnSchema> csKnownValid = List.of(CS_1, CS_2, CS_3, CS_4, CS_5);
        assertEquals(csKnownValid, table.getColumns());
    }

    @Override
    @Test
    public void readSchemaAsActualClassWithValueValidationTest() {
        final String schema = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        final MappedTableSchema table = GsonUtil.fromJson(schema, MappedTableSchema.class);
        final List<ColumnSchema> csKnownValid = List.of(CS_1, CS_2, CS_3, CS_4, CS_5);
        assertEquals(csKnownValid, table.getColumns());
    }

    /*
     * Verify that if we try to read a {@code MappedTableSchema} into a {@code PositionalTableSchema}, creation fails.
     */
    @Override
    @Test
    public void readAsWrongSchemaClassTypeTest() {
        final String schema = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, PositionalTableSchema.class));
    }

    @Override
    @Test
    public void readNullJsonInputTest() {
        assertNull(GsonUtil.fromJson("null", MappedTableSchema.class));
    }

    @Override
    @Test
    public void readWithEmptyJsonClassTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("{}", MappedTableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.MappedTableSchema.", e.getMessage());
    }

    @Override
    @Test
    public void readWithWrongJsonClassTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("true", MappedTableSchema.class));
    }

    /*
     * Verify that if {@code headerRow = false}, {@code MappedTableSchema} fails validation
     */
    @Override
    @Test
    public void readWithWrongHeaderValueTest() {
        final String schema = "{headerRow: false, columns: [" + C_1 + "," + C_2 + "," + C_3 + "]}";
        // Interesting note: When called on the MappedTableSchema class, GSON does not use TableSchemaTypeAdapter, so it's
        // validate() that catches the incorrect header type. When it's called on the TableSchema class it uses the adapter and
        // gets a syntax exception (correctly) instead.
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, MappedTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
    }

    @Override
    @Test
    public void readWithJsonNullAsColumnsTest() {
        final String schema = "{headerRow: true, columns:null}";
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, MappedTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
    }

    @Override
    @Test
    public void readWithWrongJsonTypeAsColumnsValueTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("{headerRow: true, columns:{0: " +
                CS_1 + ", 1:" + CS_2 + "}}", PositionalTableSchema.class));
    }

    @Override
    @Test
    public void verifyEmptyColumnsRejectedTest() {
        final String noColumnsSchema = "{headerRow: true, columns: []}";
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(noColumnsSchema, MappedTableSchema.class));
    }

    @Override
    @Test
    public void writeSchemaAsTableSchemaTest() {
        final TableSchema table = new MappedTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final String schemaWithClass = GsonUtil.toJson(table, TableSchema.class);
        final String schemaWithoutClass = GsonUtil.toJson(table);
        final String schemaActual = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        assertEquals(schemaActual, schemaWithClass);
        assertEquals(schemaActual, schemaWithoutClass);
    }

    @Override
    @Test
    public void writeSchemaAsActualClassTest() {
        final MappedTableSchema table = new MappedTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final String schemaWithClass = GsonUtil.toJson(table, MappedTableSchema.class);
        final String schemaWithoutClass = GsonUtil.toJson(table);
        final String schemaActual = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        assertEquals(schemaActual, schemaWithClass);
        assertEquals(schemaActual, schemaWithoutClass);
    }

    /*
     * Verify that writing {@code MappedTableSchema} to JSON fails when {@code PositionalTableSchema} is specified,
     * regardless of whether the object is referred to as a {@code TableSchema} or {@code MappedTableSchema}
     */
    @Override
    @Test
    public void writeSchemaAsWrongClassTest() {
        final TableSchema table = new MappedTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final MappedTableSchema mappedTable = new MappedTableSchema(SCHEMA_FOR_WRITE_TESTS);
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.toJson(table, PositionalTableSchema.class));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.toJson(mappedTable, PositionalTableSchema.class));
    }

    @Override
    @Test
    public void writeNullJsonInputTest() {
        assertEquals("null", GsonUtil.toJson(null, TableSchema.class));
        assertEquals("null", GsonUtil.toJson(null, MappedTableSchema.class));
    }

    /*
     * Starting with a JSON string, verify the same JSON string is returned when
     * - Reading it in as a {@code TableSchema} and writing it out as a {@code TableSchema}
     * - Reading it in as a {@code MappedTableSchema} and writing it out as a {@code TableSchema}
     * - Reading it in as a {@code TableSchema} and writing it out as a {@code MappedTableSchema}
     * - Reading it in as a {@code MappedTableSchema} and writing it out as a {@code MappedTableSchema}
     */
    @Override
    @Test
    public void roundTripStringToStringSerializationTest() {
        final String schemaIn = makeSchema(new String[]{C_1, C_2, C_3, C_4, C_5});
        final String tableSchemaInTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, TableSchema.class), TableSchema.class);
        assertEquals(schemaIn, tableSchemaInTableSchemaOut);
        final String mappedTableSchemaInTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, MappedTableSchema.class),
                TableSchema.class);
        assertEquals(schemaIn, mappedTableSchemaInTableSchemaOut);
        final String tableSchemaInMappedTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, TableSchema.class),
                MappedTableSchema.class);
        assertEquals(schemaIn, tableSchemaInMappedTableSchemaOut);
        final String mappedTableSchemaInMappedTableSchemaOut = GsonUtil.toJson(GsonUtil.fromJson(schemaIn, MappedTableSchema.class),
                MappedTableSchema.class);
        assertEquals(schemaIn, mappedTableSchemaInMappedTableSchemaOut);
    }

    /*
     * Starting with an instance of a MappedTableSchema class, verify the same value is returned when
     * - Writing it out as a {@code TableSchema} and reading it in as a {@code TableSchema}
     * - Writing it out as a {@code MappedTableSchema} and reading it in as a {@code TableSchema}
     * - Writing it out as a {@code TableSchema} and reading it in as a {@code MappedTableSchema}
     * - Writing it out as a {@code MappedTableSchema} and reading it in as a {@code MappedTableSchema}
     */
    @Override
    @Test
    public void roundTripClassToClassSerializationTest() {
        final TableSchema tableIn = new MappedTableSchema(SCHEMA_FOR_WRITE_TESTS);
        final TableSchema tableSchemaInTableSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, TableSchema.class),
                TableSchema.class);
        assertEquals(tableIn, tableSchemaInTableSchemaOut);
        final TableSchema mappedTableSchemaInTableSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, MappedTableSchema.class),
                TableSchema.class);
        assertEquals(tableIn, mappedTableSchemaInTableSchemaOut);
        final MappedTableSchema tableSchemaInMappedTableSchemaOut = GsonUtil.fromJson(GsonUtil.toJson(tableIn, TableSchema.class),
                MappedTableSchema.class);
        assertEquals(tableIn, tableSchemaInMappedTableSchemaOut);
        final MappedTableSchema mappedTableSchemaInMappedTableSchemaOut = GsonUtil.fromJson(
                GsonUtil.toJson(tableIn, MappedTableSchema.class), MappedTableSchema.class);
        assertEquals(tableIn, mappedTableSchemaInMappedTableSchemaOut);
    }
}
