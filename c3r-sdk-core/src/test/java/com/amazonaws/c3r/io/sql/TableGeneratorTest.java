// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.sql;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.utils.FileUtil.isWindows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableGeneratorTest {
    private Statement statement;

    private TableSchema schema;

    @BeforeEach
    public void setup() throws SQLException {
        statement = mock(Statement.class);
        when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenAnswer((Answer<String>) invocation -> {
            final Object[] args = invocation.getArguments();
            return "\"" + args[0] + "\""; // enquote the column names
        });

        final List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(GeneralTestUtility.cleartextColumn("source", "target1"));
        columnSchemas.add(GeneralTestUtility.sealedColumn("source", "target2", PadType.FIXED, 100));
        columnSchemas.add(GeneralTestUtility.fingerprintColumn("source", "target3"));

        schema = new MappedTableSchema(columnSchemas);
    }

    @Test
    public void generateNonceHeaderTest() {
        final String nonceBaseName = "nonce_row";
        final Set<ColumnSchema> columnSchemas = new HashSet<>();
        // if there's no columns we're guaranteed to get the "default" nonce header:
        final ColumnHeader defaultNonceHeader = TableGenerator.generateUniqueHeader(new HashSet<>(), nonceBaseName);

        // add some silly, obviously not the nonce header columns
        columnSchemas.add(GeneralTestUtility.cleartextColumn("not_nonce_but_header"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("not_nonce_but_title"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("not_nonce_but_other"));
        ColumnHeader otherNonceHeader = TableGenerator.generateUniqueHeader(columnSchemas.stream()
                        .map(ColumnSchema::getTargetHeader)
                        .collect(Collectors.toUnmodifiableSet()),
                nonceBaseName);
        // check that we get the same header since there are no name conflicts yet
        assertEquals(defaultNonceHeader, otherNonceHeader);
        // check that the nonce is indeed not in the original column set
        assertFalse(columnSchemas.stream().map(ColumnSchema::getTargetHeader).collect(Collectors.toSet()).contains(otherNonceHeader));

        // Test that we can add many possible collisions, and we always get fresh
        // nonce headers (by adding the previously generated nonce header each loop)
        for (int i = 0; i < 100; i++) {
            // add the last nonce header to the column list to introduce another conflict
            columnSchemas.add(GeneralTestUtility.cleartextColumn(otherNonceHeader.toString()));
            // generate a _new_ nonce header
            otherNonceHeader = TableGenerator.generateUniqueHeader(columnSchemas.stream()
                            .map(ColumnSchema::getTargetHeader)
                            .collect(Collectors.toUnmodifiableSet()),
                    nonceBaseName);
            // check that the new nonce header is still not the list
            assertFalse(columnSchemas.stream().map(ColumnSchema::getTargetHeader).collect(Collectors.toSet()).contains(otherNonceHeader));
        }
    }

    @Test
    public void createTableTest() throws SQLException {
        final Connection connection = TableGenerator.initTable(
                        GeneralTestUtility.CONFIG_SAMPLE,
                        new ColumnHeader("nonce"),
                        FileUtil.CURRENT_DIR)
                .getConnection();
        final String url = connection.getMetaData().getURL().replaceFirst("jdbc:sqlite:", "");
        final File dbFile = new File(url);
        assertTrue(dbFile.exists());
    }

    @Test
    public void initTableFileTest() {
        final File dbFile = TableGenerator.initTableFile(FileUtil.CURRENT_DIR);
        assertTrue(dbFile.exists());
    }

    @Test
    public void initTableFileBadDirTest() {
        assertThrows(C3rRuntimeException.class, () -> TableGenerator.initTableFile(FileUtil.TEMP_DIR + FileUtil.TEMP_DIR));
    }

    @Test
    public void initTableFileFilePathTooLongOnWindowsTest() {
        if (isWindows()) {
            final byte[] filePathBytes = new byte[500];
            Arrays.fill(filePathBytes, (byte) 'a');
            final String longFilePath = new String(filePathBytes, StandardCharsets.UTF_8);
            assertThrows(C3rRuntimeException.class, () -> TableGenerator.initTableFile(longFilePath));
        }
    }

    @Test
    public void initTableTargetColumnHeaderBadSqlHeaderTest() {
        final char[] sqlHeaderTooLong = new char[Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH];
        Arrays.fill(sqlHeaderTooLong, 'a');

        final List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(GeneralTestUtility.cleartextColumn("source", String.valueOf(sqlHeaderTooLong)));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("source", "_Starts_With_Illegal_Char"));

        schema = new MappedTableSchema(columnSchemas);

        assertDoesNotThrow(() -> TableGenerator.initTable(
                schema,
                new ColumnHeader("nonce"),
                FileUtil.CURRENT_DIR));
    }

    @Test
    public void getTableSchemaFromConfigTest() throws SQLException {
        final List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(GeneralTestUtility.sealedColumn("firstname", "firstname", PadType.FIXED, 100));
        columnSchemas.add(GeneralTestUtility.fingerprintColumn("lastname", "lastname"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("address", "address"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("city", "city"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("state", "state"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("phonenumber", "phonenumber"));
        columnSchemas.add(GeneralTestUtility.fingerprintColumn("title", "title"));
        columnSchemas.add(GeneralTestUtility.cleartextColumn("level", "level"));
        columnSchemas.add(GeneralTestUtility.sealedColumn("notes", "notes", PadType.MAX, 100));
        final Statement statement = mock(Statement.class);
        when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenAnswer((Answer<String>) invocation -> {
            final Object[] args = invocation.getArguments();
            return "\"" + args[0] + "\""; // enquote the column names
        });
        final TableSchema tableConfig = new MappedTableSchema(columnSchemas);
        final String tableSchema = TableGenerator.getTableSchemaFromConfig(
                statement, tableConfig, new ColumnHeader("nonce"));
        final StringBuilder expectedSchema = new StringBuilder("CREATE TABLE c3rTmp (\n\"nonce\" TEXT");
        for (ColumnSchema column : columnSchemas) {
            expectedSchema.append(",\n\"").append(column.getInternalHeader()).append("\" TEXT");
        }
        expectedSchema.append(")");
        assertEquals(expectedSchema.toString(), tableSchema);
    }

    @Test
    public void oneSourceToMultipleTargetsTest() {
        final String tableSchema = TableGenerator.getTableSchemaFromConfig(
                statement, schema, new ColumnHeader("nonce"));
        final StringBuilder expectedSchema = new StringBuilder("CREATE TABLE c3rTmp (\n\"nonce\" TEXT");
        for (ColumnSchema column : schema.getColumns()) {
            expectedSchema.append(",\n\"").append(column.getInternalHeader()).append("\" TEXT");
        }
        expectedSchema.append(")");
        assertEquals(expectedSchema.toString(), tableSchema);
    }

    @Test
    public void getIndexStatementTest() {
        assertEquals(
                "CREATE UNIQUE INDEX \"row_nonce_idx\" ON \"c3rTmp\"(\"nonce\", \"target1\", \"target2\", \"target3\");",
                TableGenerator.getCoveringIndexStatement(statement, schema, new ColumnHeader("nonce"))
        );
    }
}
