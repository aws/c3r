// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.action.CsvRowMarshaller;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.ReflectionMemberAccessor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MappedTableSchemaTest implements TableSchemaCommonTestInterface {
    private static final ColumnSchema CS_S_1_TO_UNNAMED_CLEARTEXT = GeneralTestUtility.cleartextColumn("S1", null);

    private static final ColumnSchema CS_S_1_TO_UNNAMED_CLEARTEXT_FINAL = GeneralTestUtility.cleartextColumn("S1", "s1");

    private static final ColumnSchema CS_S_1_TO_UNNAMED_FINGERPRINT = GeneralTestUtility.fingerprintColumn("S1", null);

    private static final ColumnSchema CS_S_1_TO_UNNAMED_FINGERPRINT_FINAL = GeneralTestUtility.fingerprintColumn("S1", "s1" +
            ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX);

    private static final ColumnSchema CS_S_1_TO_UNNAMED_SEALED = GeneralTestUtility.sealedColumn("S1", null, PadType.FIXED, 50);

    private static final ColumnSchema CS_S_1_TO_UNNAMED_SEALED_FINAL = GeneralTestUtility.sealedColumn("S1", "s1" +
                    ColumnHeader.DEFAULT_SEALED_SUFFIX, PadType.FIXED, 50);

    private static final ColumnSchema CS_S_1_TO_T_1 = GeneralTestUtility.cleartextColumn("S1", "T1");

    private static final ColumnSchema CS_S_2_TO_T_1 = GeneralTestUtility.cleartextColumn("S2", "T1");

    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    // Verify that target headers are created automatically when unspecified in the schema
    @Override
    @Test
    public void verifyAutomaticHeaderConstructionTest() {
        final var createdTargetHeaders = new MappedTableSchema(List.of(CS_S_1_TO_UNNAMED_CLEARTEXT, CS_S_1_TO_UNNAMED_FINGERPRINT,
                CS_S_1_TO_UNNAMED_SEALED));
        final var knownGoodValues = List.of(CS_S_1_TO_UNNAMED_CLEARTEXT_FINAL, CS_S_1_TO_UNNAMED_FINGERPRINT_FINAL,
                CS_S_1_TO_UNNAMED_SEALED_FINAL);
        assertEquals(knownGoodValues, createdTargetHeaders.getColumns());
    }

    // Confirm one source column can be mapped to many output columns
    @Override
    @Test
    public void oneSourceToManyTargetsTest() {
        final var schema = new MappedTableSchema(List.of(CS_S_1_TO_T_1, CS_S_1_TO_UNNAMED_CLEARTEXT));
        assertTrue(schema.getColumns().stream().map(ColumnSchema::getSourceHeader).map(ColumnHeader::toString).allMatch("s1"::equals));
        assertEquals(schema.getColumns().size(), schema.getColumns().stream().map(ColumnSchema::getTargetHeader).distinct().count());
    }

    // Confirm that if a target header is reused, the mapping is not accepted.
    @Override
    @Test
    public void repeatedTargetFailsTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> new MappedTableSchema(List.of(CS_S_1_TO_T_1, CS_S_2_TO_T_1)));
        assertEquals("Target header name can only be used once. Duplicates found: t1", e.getMessage());
    }

    // Confirm that if columns is null, the schema is rejected.
    @Override
    @Test
    public void validateNullColumnValueIsRejectedTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> new MappedTableSchema(null));
        assertEquals("At least one data column must provided in the config file.", e.getMessage());
    }

    // Confirm that if an empty schema is used, the schema is rejected
    @Override
    @Test
    public void validateEmptyColumnValueIsRejectedTest() {
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> new MappedTableSchema(List.of()));
        assertEquals("At least one data column must provided in the config file.", e.getMessage());

    }

    // Check equality operator and hashing work as expected
    @Override
    @Test
    public void equalsAndHashTest() {
        final MappedTableSchema m1 = new MappedTableSchema(List.of(CS_S_1_TO_T_1));
        final MappedTableSchema m2 = new MappedTableSchema(List.of(CS_S_1_TO_T_1));
        final MappedTableSchema m3 = new MappedTableSchema(List.of(CS_S_1_TO_UNNAMED_FINGERPRINT));
        final MappedTableSchema m1Copy = new MappedTableSchema(m1.getColumns());
        final TableSchema p = new PositionalTableSchema(List.of(List.of(GeneralTestUtility.cleartextColumn(null, "t1"))));
        final TableSchema mP = new MappedTableSchema(p.getColumns());
        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
        assertNotEquals(m1, m3);
        assertNotEquals(m1.hashCode(), m3.hashCode());
        assertEquals(m1, m1Copy);
        assertEquals(m1.hashCode(), m1Copy.hashCode());
        assertNotEquals(p, mP);
    }

    // Verify header row value must be true for Mapped schemas to work.
    @Override
    @Test
    public void verifyHeaderRowValueTest() {
        final MappedTableSchema schema = new MappedTableSchema(List.of(CS_S_1_TO_T_1, CS_S_1_TO_UNNAMED_CLEARTEXT));
        schema.setHeaderRowFlag(false);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Mapped Table Schemas require a header row in the data.", e.getMessage());
    }

    // Confirm getColumns returns the ones in the schema specification, not the file.
    @Override
    @Test
    public void verifyColumnsInResultsTest() {
        final MappedTableSchema schema = new MappedTableSchema(List.of(GeneralTestUtility.cleartextColumn("firstname")));
        final Path targetFile = tempDir.resolve("verifyGetColumnsReturnValue.csv");
        final EncryptConfig config = EncryptConfig.builder()
                .secretKey(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getKey())
                .sourceFile(GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE.getInput())
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
        final var values = CsvTestUtility.readRows(targetFile.toString());
        for (var r : values) {
            assertEquals(1, r.size());
            assertTrue(r.containsKey("firstname"));
        }
    }

    // Make sure mapped columns are correctly interpreted.
    @Override
    @Test
    public void verifyGetColumnsTest() throws IllegalAccessException, NoSuchFieldException {
        final var schema = mock(MappedTableSchema.class);
        doCallRealMethod().when(schema).getColumns();
        doCallRealMethod().when(schema).validate();
        final var rma = new ReflectionMemberAccessor();
        final Field validatedColumns = MappedTableSchema.class.getDeclaredField("validatedColumns");
        rma.set(validatedColumns, schema, null);
        final Field columns = MappedTableSchema.class.getDeclaredField("columns");

        final List<ColumnSchema> nullSource = List.of(GeneralTestUtility.cleartextColumn(null, "target"));
        rma.set(columns, schema, nullSource);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Mapped Table Schemas require a header row in the data.", e.getMessage());

        rma.set(validatedColumns, schema, null);
        final List<ColumnSchema> makeTarget = List.of(GeneralTestUtility.fingerprintColumn("source", null));
        rma.set(columns, schema, makeTarget);
        final var finalizedCols = schema.getColumns();
        assertEquals(1, finalizedCols.size());
        assertEquals(new ColumnHeader("source" + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX), finalizedCols.get(0).getTargetHeader());

        rma.set(validatedColumns, schema, null);
        final List<ColumnSchema> completeTarget = List.of(GeneralTestUtility.sealedColumn("source", "target", PadType.FIXED, 50));
        rma.set(columns, schema, completeTarget);
        final var unchangedCols = schema.getColumns();
        assertEquals(1, unchangedCols.size());
        assertEquals(completeTarget.get(0).getTargetHeader(), unchangedCols.get(0).getTargetHeader());
    }

    // Make sure unspecified headers is empty since this is a mapped schema so everything must be specified.
    @Override
    @Test
    public void verifyGetUnspecifiedHeadersReturnValueTest() {
        final MappedTableSchema schema = new MappedTableSchema(List.of(CS_S_1_TO_UNNAMED_FINGERPRINT, CS_S_1_TO_UNNAMED_SEALED));
        assertNull(schema.getPositionalColumnHeaders());
    }

    // Verify missing a source header leads to failure but missing a target header does not.
    @Override
    @Test
    public void verifyChildSpecificInvalidHeaderConstructionTest() {
        final ColumnSchema invalid = ColumnSchema.builder().targetHeader(new ColumnHeader("target")).type(ColumnType.CLEARTEXT).build();
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, () -> new MappedTableSchema(List.of(invalid)));
        assertEquals("Source header is required.", e.getMessage());
        final ColumnSchema needsTarget = ColumnSchema.builder().sourceHeader(new ColumnHeader("source")).type(ColumnType.FINGERPRINT)
                .build();
        final var schema = new MappedTableSchema(List.of(needsTarget));
        final var columns = schema.getColumns();
        assertEquals(1, columns.size());
        final var column = columns.get(0);
        assertEquals("source", column.getSourceHeader().toString());
        assertEquals("source" + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX, column.getTargetHeader().toString());
    }

    /*
     * Confirm validate() checks the required properties of a mapped type:
     * - headerRow is not null and is true
     * - No unspecified source headers are present
     */
    @Override
    @Test
    public void checkClassSpecificVerificationTest() throws NoSuchFieldException, IllegalAccessException {
        final var schema = mock(MappedTableSchema.class);
        final var rma = new ReflectionMemberAccessor();
        final Field headerRow = TableSchema.class.getDeclaredField("headerRow");
        doCallRealMethod().when(schema).validate();

        rma.set(headerRow, schema, null);
        final Exception e1 = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Mapped Table Schemas require a header row in the data.", e1.getMessage());

        rma.set(headerRow, schema, false);
        final Exception e2 = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Mapped Table Schemas require a header row in the data.", e2.getMessage());

        rma.set(headerRow, schema, true);
        when(schema.getPositionalColumnHeaders()).thenReturn(List.of(CS_S_1_TO_T_1.getSourceHeader()));
        when(schema.getHeaderRowFlag()).thenReturn(true);
        final Exception e3 = assertThrowsExactly(C3rIllegalArgumentException.class, schema::validate);
        assertEquals("Mapped schemas should not have any unspecified input headers.", e3.getMessage());
    }
}
