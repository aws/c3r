// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.ReflectionMemberAccessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Test class for implementation done in TableSchema using mocks to be independent of child implementations.
public class TableSchemaTest {
    // Use to mock a TableSchema with specified column data and header status.
    private TableSchema generateMockTableSchema(final Boolean hasHeaders, final List<ColumnSchema> columns)
            throws NoSuchFieldException, IllegalAccessException {
        final var tableSchema = mock(TableSchema.class);
        final var rma = new ReflectionMemberAccessor();
        final Field header = TableSchema.class.getDeclaredField("headerRow");
        rma.set(header, tableSchema, hasHeaders);
        when(tableSchema.getColumns()).thenReturn(columns);
        doCallRealMethod().when(tableSchema).validate();
        return tableSchema;
    }

    // Check that logic for when preprocessing is needed is correct.
    @Test
    public void mappedSchemaRequiresPreprocessingTest() {
        final var tableSchema = mock(TableSchema.class);
        doCallRealMethod().when(tableSchema).requiresPreprocessing();

        final List<ColumnSchema> noPreProcessingSchemas = List.of(GeneralTestUtility.cleartextColumn("u1"));
        when(tableSchema.getColumns()).thenReturn(noPreProcessingSchemas);
        assertFalse(tableSchema.requiresPreprocessing());

        final List<ColumnSchema> sealedRequiresPreProcessing = List.of(GeneralTestUtility.sealedColumn("s1"),
                GeneralTestUtility.sealedColumn("s2", "t2", PadType.FIXED, 50));
        when(tableSchema.getColumns()).thenReturn(sealedRequiresPreProcessing);
        assertTrue(tableSchema.requiresPreprocessing());

        final List<ColumnSchema> fingerprintRequiresPreProcessing = List.of(GeneralTestUtility.fingerprintColumn("j1"));
        when(tableSchema.getColumns()).thenReturn(fingerprintRequiresPreProcessing);
        assertTrue(tableSchema.requiresPreprocessing());
    }

    // Make sure the schema does not allow more than the maximum number of columns.
    @Test
    public void validateTooManyColumnsTest() {
        // make a fake column list that says it's bigger than we allow
        @SuppressWarnings("unchecked")
        final ArrayList<ColumnSchema> fakeBigList = mock(ArrayList.class);
        when(fakeBigList.isEmpty()).thenReturn(false);
        when(fakeBigList.size()).thenReturn(Limits.COLUMN_COUNT_MAX);

        // make a fake table schema with the fake column list

        final var maxColumnCountSchema = mock(TableSchema.class);
        when(maxColumnCountSchema.getColumns()).thenReturn(fakeBigList);
        doCallRealMethod().when(maxColumnCountSchema).validate();

        assertEquals(Limits.COLUMN_COUNT_MAX, maxColumnCountSchema.getColumns().size());

        when(fakeBigList.size()).thenReturn(Limits.COLUMN_COUNT_MAX + 1);
        assertThrows(C3rIllegalArgumentException.class, maxColumnCountSchema::validate);
    }

    // Tests that the source and header set collapses duplicates correctly.
    @Test
    public void validateGetSourceAndTargetHeadersTest() throws NoSuchFieldException, IllegalAccessException {
        final List<ColumnSchema> columnSchemas = List.of(
                GeneralTestUtility.cleartextColumn("s1", "t1"),
                GeneralTestUtility.cleartextColumn("s1", "t2"),
                GeneralTestUtility.cleartextColumn("s2", "t3")
        );
        final var tableSchema = generateMockTableSchema(false, columnSchemas);
        doCallRealMethod().when(tableSchema).getSourceAndTargetHeaders();

        final Set<ColumnHeader> knownValid = Set.of(
                new ColumnHeader("s1"),
                new ColumnHeader("s2"),
                new ColumnHeader("t1"),
                new ColumnHeader("t2"),
                new ColumnHeader("t3")
        );
        final var results = tableSchema.getSourceAndTargetHeaders();
        assertEquals(knownValid, results);
    }

    // Checks for the same target header being reused is rejected during validations.
    @Test
    public void checkDuplicateTargetsTest() throws NoSuchFieldException, IllegalAccessException {
        final List<ColumnSchema> noDuplicates = List.of(
                GeneralTestUtility.cleartextColumn("s1", "t1"),
                GeneralTestUtility.cleartextColumn("s2", "t2")
        );
        final var tableSchema = generateMockTableSchema(true, noDuplicates);
        assertDoesNotThrow(tableSchema::validate);

        final List<ColumnSchema> duplicates = List.of(
                GeneralTestUtility.cleartextColumn("s1", "t1"),
                GeneralTestUtility.cleartextColumn("s2", "t2"),
                GeneralTestUtility.cleartextColumn("s3", "t2")
        );
        final var tableSchemaDuplicates = generateMockTableSchema(true, duplicates);
        final Exception e = assertThrows(C3rIllegalArgumentException.class, tableSchemaDuplicates::validate);
        assertEquals("Target header name can only be used once. Duplicates found: t2", e.getMessage());
    }

    // Test that a null value for the header and a null value for the columns causes construction to fail.
    @Test
    public void nullHeaderRowAndNullColumnsTest() throws NoSuchFieldException, IllegalAccessException {
        final var tableSchema = generateMockTableSchema(null, null);

        final Exception e = assertThrows(C3rIllegalArgumentException.class, tableSchema::validate);
        assertEquals("Schema was not initialized.", e.getMessage());
    }

    // Test that if the header is null the schema won't be created.
    @Test
    public void nullHeaderRowTest() throws IllegalAccessException, NoSuchFieldException {
        final var tableSchema = generateMockTableSchema(null, new ArrayList<>());

        final Exception e = assertThrows(C3rIllegalArgumentException.class, tableSchema::validate);
        assertEquals("Schema must specify whether or not data has a header row.", e.getMessage());
    }

    // Check that validation fails because no value was given for columns.
    @Test
    public void nullColumnsTest() throws NoSuchFieldException, IllegalAccessException {
        final var tableSchema = generateMockTableSchema(false, null);

        final Exception e = assertThrows(C3rIllegalArgumentException.class, tableSchema::validate);
        assertEquals("At least one data column must provided in the config file.", e.getMessage());
    }

    // Check that validations fails because the column list is empty
    @Test
    public void emptyColumnsTest() throws NoSuchFieldException, IllegalAccessException {
        final var tableSchema = generateMockTableSchema(true, new ArrayList<>());

        final Exception e = assertThrows(C3rIllegalArgumentException.class, tableSchema::validate);
        assertEquals("At least one data column must provided in the config file.", e.getMessage());
    }
}
