// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.ColumnInsight;
import com.amazonaws.c3r.io.sql.SqlTable;
import com.amazonaws.c3r.io.sql.TableGenerator;
import lombok.Getter;
import lombok.NonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class for adding rows of data to an existing table. The Connection passed to this class should be grabbed
 * from {@link TableGenerator#initTable}.
 *
 * @param <T> Data type being written to the database.
 */
public class SqlRowWriter<T extends Value> implements RowWriter<T> {
    /**
     * Generate a statement to write the next row to the database.
     */
    private final PreparedStatement insertStatement;

    /**
     * Connection to SQL database for this session.
     */
    private final Connection connection;

    /**
     * A map of columns to their (1-based) positions in the insert statement.
     */
    private final Map<ColumnHeader, Integer> columnStatementPositions = new LinkedHashMap<>();

    /**
     * Maps target column headers to internal headers.
     */
    private final Map<ColumnHeader, ColumnHeader> internalToTargetColumnHeaders;

    /**
     * Name of file to write to.
     */
    @Getter
    private final String targetName;

    /**
     * Configures a connection to a SQL database for writing data.
     *
     * @param columnInsights Metadata information about columns being written
     * @param nonceHeader    Name for column where nonces will be stored
     * @param sqlTable       SQL table  connection
     * @throws C3rRuntimeException If a connection to the SQL database couldn't be established
     */
    public SqlRowWriter(final Collection<ColumnInsight> columnInsights, final ColumnHeader nonceHeader,
                        @NonNull final SqlTable sqlTable) {
        final List<ColumnInsight> columns = new ArrayList<>(columnInsights);
        for (int i = 0; i < columnInsights.size(); i++) {
            columnStatementPositions.put(columns.get(i).getInternalHeader(), i + 1);
        }
        columnStatementPositions.put(nonceHeader, columnInsights.size() + 1); // Add the nonce column to the end
        internalToTargetColumnHeaders = columnInsights.stream()
                .collect(Collectors.toMap(ColumnSchema::getInternalHeader, ColumnSchema::getTargetHeader));
        internalToTargetColumnHeaders.put(nonceHeader, nonceHeader);
        this.connection = sqlTable.getConnection();
        this.insertStatement = initInsertStatement();
        try {
            this.targetName = connection.getCatalog();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Internal error: failed to connect to local SQL database.", e);
        }
    }

    /**
     * Generates the SQL statement used in the PreparedStatement for inserting each row of data.
     *
     * @param statement                A statement to be used purely for escaping column names
     * @param columnStatementPositions A map of column names to their desired positions in the insert statement
     * @return The SQL statement for inserting a row of data
     */
    static String getInsertStatementSql(final Statement statement, final Map<ColumnHeader, Integer> columnStatementPositions) {
        final StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(TableGenerator.DEFAULT_TABLE_NAME).append(" (");
        // Ensure columns in insert statement are properly ordered
        final List<ColumnHeader> columnNames = columnStatementPositions.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        // Append all the escaped column names
        sb.append(columnNames.stream()
                .map(column -> {
                    try {
                        return statement.enquoteIdentifier(column.toString(), true);
                    } catch (SQLException e) {
                        throw new C3rRuntimeException("Could not prepare internal statement for temporary database. Failed to " +
                                "escape column header: " + column, e);
                    }
                })
                .collect(Collectors.joining(",")));
        sb.append(")\n").append("VALUES (");
        sb.append("?,".repeat(columnNames.size() - 1));
        sb.append("?)");
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ColumnHeader> getHeaders() {
        return columnStatementPositions.keySet();
    }

    /**
     * Generates the PreparedStatement for inserting a row of data based on the columns that were provided in the TableSchema.
     *
     * @return A PreparedStatement ready for inserting into the SQL table
     * @throws C3rRuntimeException If there's an error while setting up write connection to SQL database
     */
    PreparedStatement initInsertStatement() {
        try {
            final Statement statement = connection.createStatement(); // Used strictly to escape column names
            final String sql = getInsertStatementSql(statement, columnStatementPositions);
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new C3rRuntimeException("Could not prepare internal statement for temporary database.", e);
        }
    }

    /**
     * Takes a map of all the column headers to their respective values and adds them to the SQL table.
     * <ul>
     *     <li>Map should include the nonce.</li>
     *     <li>Values not included in the map provided will be inserted as nulls.</li>
     * </ul>
     *
     * @param row A map of column headers to values to be added to the table
     */
    @Override
    public void writeRow(@NonNull final Row<T> row) {
        try {
            for (Map.Entry<ColumnHeader, Integer> entry : columnStatementPositions.entrySet()) {
                final ColumnHeader targetColumn = internalToTargetColumnHeaders.get(entry.getKey());
                insertStatement.setBytes(entry.getValue(), row.getValue(targetColumn).getBytes());
            }
            insertStatement.execute();
            insertStatement.clearParameters();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Unknown SQL error: could not add row to the temporary database. "
                    + "Please review stack traces for more detail.", e);
        }
    }

    /**
     * No op as connection may be in use elsewhere.
     */
    @Override
    public void close() {
        // Nothing to do. Can't close Connection as it may be in use elsewhere.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        // Nothing to do.
    }
}
