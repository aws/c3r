// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.ColumnInsight;
import com.amazonaws.c3r.io.sql.SqlTable;
import com.amazonaws.c3r.io.sql.TableGenerator;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlRowWriterTest {
    private static final ColumnHeader NONCE_HEADER_DEFAULT = new ColumnHeader("c3r_nonce");

    private final CsvRow row1 = GeneralTestUtility.csvRow(
            NONCE_HEADER_DEFAULT.toString(), "nonce",
            "firstname", "Ada",
            "lastname", "Lovelace",
            "address", "Ada's house",
            "city", "London",
            "state", "England",
            "phonenumber", "123-456-7890",
            "title", "Computing Pioneer",
            "level", "Countess",
            "notes", "MIL-STD-1815"
    );

    private List<ColumnInsight> columnInsights;

    private Map<ColumnHeader, Integer> columnStatementPositions;

    private Map<ColumnHeader, ColumnHeader> internalToTargetColumnHeaders;

    private TableSchema tableSchema;

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

        columnInsights = columnSchemas.stream().map(ColumnInsight::new).collect(Collectors.toList());

        columnStatementPositions = new HashMap<>();
        for (int i = 0; i < columnInsights.size(); i++) {
            columnStatementPositions.put(columnInsights.get(i).getInternalHeader(), i + 1);
        }
        columnStatementPositions.put(NONCE_HEADER_DEFAULT, columnInsights.size() + 1);
        tableSchema = new MappedTableSchema(columnSchemas);
        sqlTable = null;
        internalToTargetColumnHeaders = columnInsights.stream()
                .collect(Collectors.toMap(ColumnSchema::getTargetHeader, ColumnSchema::getInternalHeader));
        internalToTargetColumnHeaders.put(NONCE_HEADER_DEFAULT, NONCE_HEADER_DEFAULT);
    }

    private void initTable() {
        sqlTable = TableGenerator.initTable(tableSchema, NONCE_HEADER_DEFAULT, FileUtil.CURRENT_DIR);
    }

    @Test
    public void getHeaderTest() {
        initTable();
        final SqlRowWriter<CsvValue> sqlRecordWriter = new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable);
        assertEquals(new HashSet<>(sqlRecordWriter.getHeaders()), columnStatementPositions.keySet());
    }

    @Test
    public void getInsertStatementSqlTest() throws SQLException {
        initTable();
        final Statement statement = mock(Statement.class);
        when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenAnswer((Answer<String>) invocation -> {
            final Object[] args = invocation.getArguments();
            return "\"" + args[0] + "\""; // enquote the column names
        });
        final String insertStatement = SqlRowWriter.getInsertStatementSql(statement, columnStatementPositions);
        final StringBuilder expectedInsertStatement = new StringBuilder("INSERT INTO ").append(TableGenerator.DEFAULT_TABLE_NAME)
                .append(" (");
        for (ColumnInsight column : columnInsights) {
            expectedInsertStatement.append("\"").append(column.getInternalHeader()).append("\",");
        }
        expectedInsertStatement.append("\"").append(NONCE_HEADER_DEFAULT).append("\")\nVALUES (?,?,?,?,?,?,?,?,?,?)");
        assertEquals(expectedInsertStatement.toString(), insertStatement);
    }

    @Test
    public void getInsertStatementSqlSqlExceptionTest() throws SQLException {
        initTable();
        final Statement statement = mock(Statement.class);
        when(statement.enquoteIdentifier(anyString(), anyBoolean())).thenThrow(SQLException.class);
        assertThrows(C3rRuntimeException.class, () -> SqlRowWriter.getInsertStatementSql(statement, columnStatementPositions));
    }

    @Test
    public void initInsertStatementSqlExceptionTest() throws SQLException {
        initTable();
        sqlTable = mock(SqlTable.class);
        final Connection connection = mock(Connection.class);
        when(sqlTable.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenThrow(SQLException.class);
        assertThrows(C3rRuntimeException.class, () -> new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable));
    }

    @Test
    public void closeDoesNotCloseConnectionTest() throws SQLException {
        initTable();
        final SqlRowWriter<CsvValue> sqlRecordWriter = new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable);
        assertFalse(sqlTable.getConnection().isClosed());
        sqlRecordWriter.close();
        assertFalse(sqlTable.getConnection().isClosed());
    }

    @Test
    public void writeRowTest() throws SQLException {
        initTable();
        final SqlRowWriter<CsvValue> sqlRecordWriter = new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable);
        sqlRecordWriter.writeRow(row1);
        final String selectSql = "SELECT * FROM " + TableGenerator.DEFAULT_TABLE_NAME;
        try (Statement statement = sqlTable.getConnection().createStatement()) {
            final ResultSet rs = statement.executeQuery(selectSql);
            for (var column : row1.getHeaders()) {
                assertArrayEquals(row1.getValue(column).getBytes(), rs.getBytes(internalToTargetColumnHeaders.get(column).toString()));
            }
        }
    }

    @Test
    public void writeRowNullValuesTest() throws SQLException {
        initTable();
        // clone row1, modify some entries to be blank
        final Row<CsvValue> row = new CsvRow(row1);
        row.putValue(new ColumnHeader("address"), new CsvValue((String) null));
        row.putValue(new ColumnHeader("notes"), new CsvValue((String) null));
        final SqlRowWriter<CsvValue> sqlRecordWriter = new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable);
        sqlRecordWriter.writeRow(row);
        final String selectSql = "SELECT * FROM " + TableGenerator.DEFAULT_TABLE_NAME;
        try (Statement statement = sqlTable.getConnection().createStatement()) {
            final ResultSet rs = statement.executeQuery(selectSql);
            for (var column : row.getHeaders()) {
                assertArrayEquals(row.getValue(column).getBytes(), rs.getBytes(internalToTargetColumnHeaders.get(column).toString()));
            }
            // These two columns were omitted from the insert and should be null.
            assertNull(rs.getString(internalToTargetColumnHeaders.get(new ColumnHeader("address")).toString()));
            assertNull(rs.getString(internalToTargetColumnHeaders.get(new ColumnHeader("notes")).toString()));
        }
    }

    @Test
    public void getSourceNameTest() throws SQLException {
        sqlTable = mock(SqlTable.class);
        final Connection connection = mock(Connection.class);
        when(sqlTable.getConnection()).thenReturn(connection);
        when(connection.getCatalog()).thenReturn("the_database");
        when(connection.createStatement()).thenReturn(mock(Statement.class));
        final SqlRowWriter<CsvValue> sqlRecordWriter = new SqlRowWriter<>(columnInsights, NONCE_HEADER_DEFAULT, sqlTable);
        assertEquals(
                connection.getCatalog(),
                sqlRecordWriter.getTargetName());
    }
}
