// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.action.CsvRowMarshaller;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.ReflectionMemberAccessor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class PositionalTableSchemaTest implements TableSchemaCommonTestInterface {
    private static final ColumnSchema CS_1 = GeneralTestUtility.cleartextColumn(null, "t1");

    private static final ColumnSchema CS_2 = GeneralTestUtility.fingerprintColumn(null, "t2");

    private static final ColumnSchema CS_2_DUPLICATE_TARGET = GeneralTestUtility.fingerprintColumn(null, "t2");

    private static final ColumnSchema CS_3 = GeneralTestUtility.sealedColumn(null, "t3", PadType.NONE, null);

    private static final ColumnSchema CS_4 = GeneralTestUtility.sealedColumn(null, "t4", PadType.FIXED, 50);

    private static final ColumnSchema CS_5 = GeneralTestUtility.sealedColumn(null, "t5", PadType.MAX, 100);

    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    /*
     * Add an extra column row to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooManyRowsPositionalSchemaTest() throws IOException {
        final var csvFile = tempDir.resolve("tooManyRows.csv");
        Files.writeString(csvFile, "1,2,3\n4,5,6");
        final var schema = new PositionalTableSchema(List.of(
                List.of(GeneralTestUtility.cleartextColumn(null, "t1")),
                List.of(GeneralTestUtility.cleartextColumn(null, "t2")),
                List.of(GeneralTestUtility.cleartextColumn(null, "t3")),
                List.of(GeneralTestUtility.cleartextColumn(null, "t4"))
        ));
        final Path targetFile = tempDir.resolve("tooManyRowsPositionalSchema.csv");
        final EncryptConfig config = EncryptConfig.builder()
                .secretKey(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile(csvFile.toString())
                .targetFile(targetFile.toString())
                .tempDir(tempDir.toString())
                .salt("1234")
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final Exception e = assertThrowsExactly(C3rRuntimeException.class, () -> CsvRowMarshaller.newInstance(config));
        assertEquals("Positional table schemas must match the same number of columns as the data. Expected: 4, found: 3.", e.getMessage());
    }

    /*
     * Remove a column row to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooFewRowsPositionalSchemaTest() throws IOException {
        final var csvFile = tempDir.resolve("tooManyRows.csv");
        Files.writeString(csvFile, "1,2,3\n4,5,6");
        final var schema = new PositionalTableSchema(List.of(
                List.of(GeneralTestUtility.cleartextColumn(null, "t1")),
                List.of(GeneralTestUtility.cleartextColumn(null, "t2"))
        ));
        final Path targetFile = tempDir.resolve("tooFewRowsPositionalSchema.csv");
        final EncryptConfig config = EncryptConfig.builder()
                .secretKey(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile(csvFile.toString())
                .targetFile(targetFile.toString())
                .tempDir(tempDir.toString())
                .salt("1234")
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final Exception e = assertThrowsExactly(C3rRuntimeException.class, () -> CsvRowMarshaller.newInstance(config));
        assertEquals("Positional table schemas must match the same number of columns as the data. Expected: 2, found: 3.", e.getMessage());
    }

    // Verify that headers are correctly autogenerated as needed.
    @Override
    @Test
    public void verifyAutomaticHeaderConstructionTest() {
        final List<ColumnHeader> knownGoodHeaders = List.of(
                ColumnHeader.getColumnHeaderFromIndex(0),
                ColumnHeader.getColumnHeaderFromIndex(1),
                ColumnHeader.getColumnHeaderFromIndex(2),
                ColumnHeader.getColumnHeaderFromIndex(3),
                ColumnHeader.getColumnHeaderFromIndex(4),
                ColumnHeader.getColumnHeaderFromIndex(5)
        );
        final List<ColumnHeader> knownGoodHeadersInUse = List.of(
                ColumnHeader.getColumnHeaderFromIndex(1),
                ColumnHeader.getColumnHeaderFromIndex(2),
                ColumnHeader.getColumnHeaderFromIndex(4),
                ColumnHeader.getColumnHeaderFromIndex(5)
        );
        final List<List<ColumnSchema>> positionalColumns = List.of(
                new ArrayList<>(),
                List.of(CS_1),
                List.of(CS_2, CS_3),
                new ArrayList<>(),
                List.of(CS_4),
                List.of(CS_5)
        );
        final PositionalTableSchema schema = new PositionalTableSchema(positionalColumns);
        assertEquals(knownGoodHeaders, schema.getPositionalColumnHeaders());
        assertEquals(knownGoodHeadersInUse,
                schema.getColumns().stream().map(ColumnSchema::getSourceHeader).distinct().collect(Collectors.toList()));
    }

    // Verify one source column can be mapped to multiple output columns as long as they have unique target header names.
    @Override
    @Test
    public void oneSourceToManyTargetsTest() {
        final ColumnHeader knownGoodHeader = ColumnHeader.getColumnHeaderFromIndex(0);
        final PositionalTableSchema schema = new PositionalTableSchema(
                List.of(
                        List.of(CS_1, CS_2, CS_3, CS_4, CS_5)
                )
        );
        for (ColumnSchema c : schema.getColumns()) {
            assertEquals(knownGoodHeader, c.getSourceHeader());
        }
    }

    // Verify that is the same target header name is used more than once in a schema, it fails validation.
    @Override
    @Test
    public void repeatedTargetFailsTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> new PositionalTableSchema(List.of(List.of(CS_2, CS_2_DUPLICATE_TARGET))));
        assertEquals("Target header name can only be used once. Duplicates found: " + CS_2.getTargetHeader().toString(), e.getMessage());
    }

    // Validate that the column is null the schema is not accepted.
    @Override
    @Test
    public void validateNullColumnValueIsRejectedTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> new PositionalTableSchema(null));
        assertEquals("At least one data column must provided in the config file.", e.getMessage());
    }

    // Validate that if the output column list is empty, the schema is not accepted.
    @Override
    @Test
    public void validateEmptyColumnValueIsRejectedTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> new PositionalTableSchema(List.of(List.of(),
                List.of())));
        assertEquals("At least one data column must provided in the config file.", e.getMessage());
    }

    // Test the implementation of equals and hash to make sure results are as expected.
    @Override
    @Test
    public void equalsAndHashTest() {
        final PositionalTableSchema p1 = new PositionalTableSchema(List.of(
                List.of(CS_1, CS_2),
                List.of(CS_3, CS_4),
                List.of(CS_5)
        ));
        final PositionalTableSchema p2 = new PositionalTableSchema(List.of(
                List.of(CS_1, CS_2),
                List.of(CS_3, CS_4),
                List.of(CS_5)
        ));
        final PositionalTableSchema p3 = new PositionalTableSchema(List.of(
                List.of(CS_1, CS_2),
                List.of(CS_3, CS_4),
                List.of(CS_5),
                List.of()
        ));
        final PositionalTableSchema p4 = new PositionalTableSchema(List.of(
                List.of(CS_1, CS_2),
                List.of(),
                List.of(CS_3, CS_4),
                List.of(CS_5)
        ));
        final PositionalTableSchema p5 = new PositionalTableSchema(List.of(
                List.of(CS_1),
                List.of(CS_3),
                List.of(CS_5)
        ));
        final TableSchema m = new MappedTableSchema(List.of(GeneralTestUtility.cleartextColumn("Column 1", "t1")));
        final TableSchema pM = new PositionalTableSchema(List.of(List.of(CS_1)));
        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        assertEquals(p1, p3);
        assertEquals(p1.hashCode(), p3.hashCode());
        assertNotEquals(p3, p4);
        assertNotEquals(p3.hashCode(), p4.hashCode());
        assertNotEquals(p1, p5);
        assertNotEquals(p1.hashCode(), p5.hashCode());
        assertNotEquals(m, pM);
        assertNotEquals(m.hashCode(), pM.hashCode());
    }

    // Verify the header row can't be the wrong value and the schema will still work.
    @Override
    @Test
    public void verifyHeaderRowValueTest() {
        final PositionalTableSchema schema = new PositionalTableSchema(List.of(List.of(CS_1, CS_2)));
        schema.setHeaderRowFlag(true);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Positional Table Schemas cannot use data containing a header row", e.getMessage());
    }

    // Check the results from {@code getColumns()} matches the ones used in the schema and not the ones in the file.
    @Override
    @Test
    public void verifyColumnsInResultsTest() {
        final Set<String> knownGoodHeaders = Set.of("t1", "t2", "t3", "t4", "t5");
        final var knownGoodValues = CsvTestUtility.readContentAsArrays("../samples/csv/data_sample_no_headers.csv", true);

        final PositionalTableSchema schema = new PositionalTableSchema(List.of(
                List.of(GeneralTestUtility.cleartextColumn(null, "t1")),
                List.of(),
                List.of(GeneralTestUtility.cleartextColumn(null, "t2"), GeneralTestUtility.cleartextColumn(null, "t3")),
                List.of(),
                List.of(),
                List.of(GeneralTestUtility.cleartextColumn(null, "t4")),
                List.of(),
                List.of(),
                List.of(GeneralTestUtility.cleartextColumn(null, "t5"))

        ));
        final Path targetFile = tempDir.resolve("verifyColumnsInResultsTest.csv");
        final EncryptConfig config = EncryptConfig.builder()
                .secretKey(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile("../samples/csv/data_sample_no_headers.csv")
                .targetFile(targetFile.toString())
                .tempDir(tempDir.toString())
                .salt(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getSalt())
                .settings(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getSettings())
                .tableSchema(schema)
                .overwrite(true)
                .build();
        final RowMarshaller<CsvValue> rowMarshaller = CsvRowMarshaller.newInstance(config);
        rowMarshaller.marshal();
        rowMarshaller.close();

        final List<Map<String, String>> results = CsvTestUtility.readRows(targetFile.toString());
        final Map<String, Map<String, String>> lookupMap = new HashMap<>();
        for (var row : results) {
            lookupMap.put(row.get("t1"), row);
        }

        assertEquals(knownGoodValues.size(), lookupMap.size());
        for (String[] knownGoodRow : knownGoodValues) {
            final Map<String, String> r = lookupMap.get(knownGoodRow[0]);
            assertEquals(5, r.size());
            assertEquals(knownGoodHeaders, r.keySet());
            assertTrue(CsvTestUtility.compareCsvValues(knownGoodRow[0], r.get("t1")));
            assertTrue(CsvTestUtility.compareCsvValues(knownGoodRow[2], r.get("t2")));
            assertTrue(CsvTestUtility.compareCsvValues(knownGoodRow[2], r.get("t3")));
            assertTrue(CsvTestUtility.compareCsvValues(knownGoodRow[5], r.get("t4")));
            assertTrue(CsvTestUtility.compareCsvValues(knownGoodRow[8], r.get("t5")));
        }
    }

    // Check that getColumns() returns properly validated columns.
    @Override
    @Test
    public void verifyGetColumnsTest() throws IllegalAccessException, NoSuchFieldException {
        final var schema = mock(PositionalTableSchema.class);
        doCallRealMethod().when(schema).getColumns();
        final var rma = new ReflectionMemberAccessor();
        final Field mappedColumns = PositionalTableSchema.class.getDeclaredField("mappedColumns");
        rma.set(mappedColumns, schema, null);
        final Field columns = PositionalTableSchema.class.getDeclaredField("columns");

        final List<List<ColumnSchema>> emptyColumn = List.of(List.of(), List.of(GeneralTestUtility.cleartextColumn(null, "target")));
        rma.set(columns, schema, emptyColumn);
        final var skippedEmpty = schema.getColumns();
        assertEquals(1, skippedEmpty.size());
        assertNull(emptyColumn.get(1).get(0).getSourceHeader());
        assertEquals(ColumnHeader.getColumnHeaderFromIndex(1), skippedEmpty.get(0).getSourceHeader());
    }

    // Make sure the various states of unspecified headers are correct for the child implementation.
    @Override
    @Test
    public void verifyGetUnspecifiedHeadersReturnValueTest() throws IllegalAccessException, NoSuchFieldException {
        final List<ColumnHeader> knownGoodColumns = List.of(
                ColumnHeader.getColumnHeaderFromIndex(0),
                ColumnHeader.getColumnHeaderFromIndex(1),
                ColumnHeader.getColumnHeaderFromIndex(2)
        );
        final var schema = mock(PositionalTableSchema.class);
        doCallRealMethod().when(schema).getPositionalColumnHeaders();
        doCallRealMethod().when(schema).validate();
        final var rma = new ReflectionMemberAccessor();
        final Field headers = PositionalTableSchema.class.getDeclaredField("sourceHeaders");
        rma.set(headers, schema, null);
        final Field columns = PositionalTableSchema.class.getDeclaredField("columns");
        final List<List<ColumnSchema>> columnsWithSkippedFields = List.of(List.of(CS_1), List.of(), List.of(CS_2));
        rma.set(columns, schema, columnsWithSkippedFields);
        assertEquals(knownGoodColumns, schema.getPositionalColumnHeaders());
    }

    // Make sure child implementation rejects all invalid headers for its type.
    @Override
    @Test
    public void verifyChildSpecificInvalidHeaderConstructionTest() {
        final ColumnSchema hasSourceHeader = GeneralTestUtility.cleartextColumn("source");
        final Exception e1 = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> new PositionalTableSchema(List.of(List.of(hasSourceHeader))));
        assertEquals("Positional table schemas cannot have `sourceHeader` properties in column schema, but found one in column 1.",
                e1.getMessage());

        final ColumnSchema noTargetHeader = GeneralTestUtility.cleartextColumn(null, null);
        final Exception e2 = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> new PositionalTableSchema(List.of(List.of(noTargetHeader))));
        assertEquals("Positional table schemas must have a target header name for each column schema. Missing target header in column 1.",
                e2.getMessage());
    }

    // Check the child specific {@code verification} function to be sure it works as expected.
    @Override
    @Test
    public void checkClassSpecificVerificationTest() {
        final PositionalTableSchema schema = new PositionalTableSchema(List.of(List.of(CS_1)));
        schema.setHeaderRowFlag(true);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Positional Table Schemas cannot use data containing a header row", e.getMessage());
    }

    @Test
    public void positionalSourceHeadersTest() {
        assertTrue(PositionalTableSchema.generatePositionalSourceHeaders(0).isEmpty());
        assertEquals(
                List.of(ColumnHeader.getColumnHeaderFromIndex(0), ColumnHeader.getColumnHeaderFromIndex(1)),
                PositionalTableSchema.generatePositionalSourceHeaders(2));
    }
}
