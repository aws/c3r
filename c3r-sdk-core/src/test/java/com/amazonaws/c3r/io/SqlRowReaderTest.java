// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.CsvValueFactory;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.sql.SqlTable;
import com.amazonaws.c3r.io.sql.TableGenerator;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlRowReaderTest {
    private static final ColumnHeader NONCE_HEADER_DEFAULT = new ColumnHeader("c3r_nonce");

    private List<ColumnInsight> columnInsights;

    private List<ColumnHeader> columnNames;

    private SqlTable sqlTable;

    @BeforeEach
    public void setup() {
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

        columnInsights = columnSchemas.stream().map(x -> new ColumnInsight(x, ClientSettings.lowAssuranceMode()))
                .collect(Collectors.toList());

        columnNames = columnInsights.stream().map(ColumnSchema::getTargetHeader).collect(Collectors.toList());

        final TableSchema tableSchema = new MappedTableSchema(columnSchemas);
        sqlTable = TableGenerator.initTable(tableSchema, NONCE_HEADER_DEFAULT, FileUtil.CURRENT_DIR);
    }

    @Test
    public void getHeadersTest() {
        final SqlRowReader<CsvValue> sqlRowReader = new SqlRowReader<>(
                columnInsights,
                NONCE_HEADER_DEFAULT,
                mock(CsvValueFactory.class),
                sqlTable);
        assertEquals(
                sqlRowReader.getHeaders(),
                columnInsights.stream().map(ColumnSchema::getInternalHeader).collect(Collectors.toList()));
    }

    @Test
    public void getSourceNameTest() throws SQLException {
        final SqlRowReader<CsvValue> sqlRowReader = new SqlRowReader<>(
                columnInsights,
                NONCE_HEADER_DEFAULT,
                mock(CsvValueFactory.class),
                sqlTable);
        assertEquals(
                sqlTable.getConnection().getCatalog(),
                sqlRowReader.getSourceName());
    }

    @Test
    public void getInsertStatementSqlTest() throws SQLException {
        final Statement statement = mock(Statement.class);
        when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenAnswer((Answer<String>) invocation -> {
            final Object[] args = invocation.getArguments();
            return "\"" + args[0] + "\""; // enquote the column names
        });
        final String insertStatement = SqlRowReader.getSelectStatementSql(statement, columnNames, NONCE_HEADER_DEFAULT);
        final String expectedInsertStatement = "SELECT \"firstname\",\"lastname\",\"address\",\"city\",\"state\",\"phonenumber\""
                + ",\"title\",\"level\",\"notes\",\"" + NONCE_HEADER_DEFAULT + "\" FROM " + TableGenerator.DEFAULT_TABLE_NAME
                + " ORDER BY \"" + NONCE_HEADER_DEFAULT + "\"";
        assertEquals(expectedInsertStatement, insertStatement);
    }

    @Test
    public void getInsertStatementSqlSqlExceptionTest() throws SQLException {
        try (Statement statement = mock(Statement.class)) {
            when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenThrow(SQLException.class);
            assertThrows(C3rRuntimeException.class, () -> SqlRowReader.getSelectStatementSql(statement, columnNames, NONCE_HEADER_DEFAULT));
        }
    }

    @Test
    public void initInsertStatementSqlExceptionTest() throws SQLException {
        sqlTable = mock(SqlTable.class);
        final Connection connection = mock(Connection.class);
        when(sqlTable.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenThrow(SQLException.class);
        assertThrows(C3rRuntimeException.class, () -> new SqlRowReader<>(
                columnInsights,
                NONCE_HEADER_DEFAULT,
                mock(CsvValueFactory.class),
                sqlTable));
    }

    @Test
    public void closeDoesNotCloseConnectionTest() throws SQLException {
        final SqlRowReader<CsvValue> sqlRowReader = new SqlRowReader<>(
                columnInsights,
                NONCE_HEADER_DEFAULT,
                mock(CsvValueFactory.class),
                sqlTable);
        assertFalse(sqlTable.getConnection().isClosed());
        sqlRowReader.close();
        assertFalse(sqlTable.getConnection().isClosed());
    }

    @Test
    public void readRowsOrderByTest() throws SQLException {
        // Grab a couple column headers. What they are doesn't matter.
        final ColumnHeader firstHeader = columnInsights.get(0).getInternalHeader();
        final ColumnHeader secondHeader = columnInsights.get(1).getInternalHeader();

        try (Statement statement = sqlTable.getConnection().createStatement()) {
            String insertSql = "INSERT INTO " + TableGenerator.DEFAULT_TABLE_NAME + " (\"" + firstHeader
                    + "\", \"" + secondHeader + "\", \"" + NONCE_HEADER_DEFAULT
                    + "\") VALUES (\"John\", \"Smith\", \"00000000000000000000000000000002\")";
            statement.execute(insertSql);
            insertSql = "INSERT INTO " + TableGenerator.DEFAULT_TABLE_NAME + " (\"" + firstHeader
                    + "\", \"" + secondHeader + "\", \"" + NONCE_HEADER_DEFAULT
                    + "\") VALUES (\"Jane\", \"Mac\", \"00000000000000000000000000000001\")";
            statement.execute(insertSql);
            insertSql = "INSERT INTO " + TableGenerator.DEFAULT_TABLE_NAME + " (\"" + firstHeader
                    + "\", \"" + secondHeader + "\", \"" + NONCE_HEADER_DEFAULT
                    + "\") VALUES (\"Frank\", \"Beans\", \"00000000000000000000000000000003\")";
            statement.execute(insertSql);
        }

        final SqlRowReader<CsvValue> sqlRowReader = new SqlRowReader<>(
                columnInsights,
                NONCE_HEADER_DEFAULT,
                new CsvValueFactory(),
                sqlTable);
        byte[] largestNonce = new byte[]{};
        while (sqlRowReader.hasNext()) {
            final Row<CsvValue> row = sqlRowReader.next();
            final byte[] currentNonce = row.getValue(NONCE_HEADER_DEFAULT).getBytes();
            assertTrue(Arrays.compare(largestNonce, currentNonce) < 0);
            largestNonce = currentNonce;
        }
        assertEquals(3L, sqlRowReader.getReadRowCount());
    }
}
