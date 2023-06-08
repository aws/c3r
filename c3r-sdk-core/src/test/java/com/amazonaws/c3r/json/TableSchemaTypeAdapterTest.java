// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

// Tests top level adapter logic for schema generation to and from jsons.
public final class TableSchemaTypeAdapterTest {
    // Make sure headerRow must be specified.
    @Test
    public void verifyHeaderRowRequiredTest() {
        final String schema = "{\"columns\": []}";
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.TableSchema.", e.getMessage());
    }

    @Test
    public void verifyObjectStatusTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("2", TableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.TableSchema.", e.getMessage());
    }

    // Make sure headerRow is a boolean.
    @Test
    public void readWithWrongJsonTypeAsHeaderValueTest() {
        final String schema1 = "{headerRow: 1, columns: {}}";
        final Exception e1 = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema1, TableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.TableSchema.", e1.getMessage());
        final String schema2 = "{headerRow: {a: 1, b: 2}, columns: {}}";
        final Exception e2 = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema2, TableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.TableSchema.", e2.getMessage());
    }

    // Make sure null value for headerRow is rejected.
    @Test
    public void readWithJsonNullAsHeaderValueTest() {
        final String schema = "{headerRow: null, columns: {}}";
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));
        assertEquals("Unable to parse JSON class com.amazonaws.c3r.config.TableSchema.", e.getMessage());
    }

    // Make sure null value for headerRow is rejected.
    @Test
    public void readWithWrongClassValueTest() {
        final String schema = "{headerRow: true, columns: {}}";
        final var settings = GsonUtil.fromJson(schema, ClientSettings.class);
        assertEquals(ClientSettings.highAssuranceMode(), settings);
    }

    // Make sure we return null when value is null
    @Test
    public void readWithJsonNullReturnsNullTest() {
        final String schema = "null";
        assertNull(GsonUtil.fromJson(schema, TableSchema.class));
        assertNull(GsonUtil.fromJson(null, TableSchema.class));
    }

    @Test
    public void readWithEmptyJsonClassTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("{}", TableSchema.class));
    }

    // Make sure we write a null value when the schema is null
    @Test
    public void writeWithJsonNullReturnsNullTest() {
        assertEquals("null", GsonUtil.toJson(null, TableSchema.class));
    }

    // Make sure null value for headerRow is rejected.
    @Test
    public void writeWithWrongClassValueTest() {
        final TableSchema schema = new MappedTableSchema(List.of(
                ColumnSchema.builder().sourceHeader(new ColumnHeader("source")).type(ColumnType.CLEARTEXT).build()
        ));
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> GsonUtil.toJson(schema, ClientSettings.class));
        assertEquals("Unable to write class com.amazonaws.c3r.config.ClientSettings as JSON.", e.getMessage());
    }

    // Make sure a positional schema that maps input columns to output columns produces the same relative input.
    @Test
    public void verifyMappedAndPositionalSchemaGenerateSameOutputColumnsTest() {
        final int repeatedInputColumnIndex = 5;
        final TableSchema positionalSchema = GsonUtil.fromJson(
                FileUtil.readBytes("../samples/schema/config_sample_no_headers.json"), TableSchema.class);
        final TableSchema mappedSchema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"),
                TableSchema.class);
        final var positionalColumns = positionalSchema.getColumns();
        final var mappedColumns = mappedSchema.getColumns();
        assertEquals(positionalColumns.size(), mappedColumns.size());
        for (int i = 0; i < positionalColumns.size(); i++) {
            final var positional = positionalColumns.get(i);
            final var mapped = mappedColumns.get(i);
            assertNotEquals(positional.getSourceHeader(), mapped.getSourceHeader());
            if (i == repeatedInputColumnIndex) {
                assertEquals(positional.getSourceHeader(), positionalColumns.get(i + 1).getSourceHeader());
                assertEquals(positional.getSourceHeader(), positionalColumns.get(i + 2).getSourceHeader());
                assertEquals(mapped.getSourceHeader(), mappedColumns.get(i + 1).getSourceHeader());
                assertEquals(mapped.getSourceHeader(), mappedColumns.get(i + 2).getSourceHeader());
            }
            assertEquals(positional.getTargetHeader(), mapped.getTargetHeader());
            assertEquals(positional.getPad(), mapped.getPad());
            assertEquals(positional.getType(), mapped.getType());
        }
    }

    // Check that duplicate target columns are rejected regardless of normalization-irrelevant differences
    @Test
    public void checkDuplicateTargetColumnsErrorsTst() {
        final String schema = String.join("\n", "{headerRow: true,",
                "columns: [",
                "{",
                "  \"sourceHeader\": \"FirstName\",",
                "  \"targetHeader\": \"FirstName\",",
                "  \"type\": \"cleartext\"",
                "},",
                "{",
                "  \"sourceHeader\": \"FirstName\",",
                "  \"targetHeader\": \" firstname \",",
                "  \"type\": \"cleartext\"",
                "}",
                "]}");
        assertThrows(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(schema, TableSchema.class));

        assertDoesNotThrow(() -> GsonUtil.fromJson(schema.replaceAll(" firstname ", "firstname2"), TableSchema.class));
    }
}
