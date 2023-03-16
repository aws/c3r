// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.RowFactory;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.io.sql.SqlTable;
import com.amazonaws.c3r.io.sql.TableGenerator;
import lombok.Getter;
import lombok.NonNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads a row of the specified data type out of a SQL database.
 *
 * @param <T> Type of data stored in the database.
 */
public class SqlRowReader<T extends Value> extends RowReader<T> {
    /**
     * Connection to the SQL database for this session.
     */
    private final SqlTable sqlTable;

    /**
     * Generate a statement to query the database for the next row.
     */
    private final PreparedStatement selectStatement;

    /**
     * List of internal column headers.
     */
    @Getter
    private final List<ColumnHeader> headers;

    /**
     * Result of a query to the database.
     */
    private final ResultSet resultSet;

    /**
     * Creates an empty row for the specified data type.
     */
    private final RowFactory<T> rowFactory;

    /**
     * Name of the column that contains the nonce for each row.
     */
    private final ColumnHeader nonceHeader;

    /**
     * Name of the source file.
     */
    @Getter
    private final String sourceName;

    /**
     * Maps internal column headers to target headers.
     */
    private final Map<ColumnHeader, ColumnHeader> internalToTargetColumnHeaders;

    /**
     * Holds the next row of data to be read.
     */
    private Row<T> nextRow;

    /**
     * Configures a connection to a SQL database to read out rows of data.
     *
     * @param columnInsights Metadata about columns
     * @param nonceHeader    Name of the column in the database that contains the nonce value for each row
     * @param rowFactory     Creates empty rows of the specified type
     * @param sqlTable       SQL database instance
     * @throws C3rRuntimeException If the SQL database couldn't be accessed
     */
    public SqlRowReader(@NonNull final Collection<ColumnInsight> columnInsights,
                        @NonNull final ColumnHeader nonceHeader,
                        @NonNull final RowFactory<T> rowFactory,
                        @NonNull final SqlTable sqlTable) {
        this.rowFactory = rowFactory;
        this.sqlTable = sqlTable;
        this.internalToTargetColumnHeaders = columnInsights.stream()
                .collect(Collectors.toMap(ColumnSchema::getInternalHeader, ColumnSchema::getTargetHeader));
        this.headers = columnInsights.stream().map(ColumnSchema::getInternalHeader).collect(Collectors.toList());
        this.nonceHeader = nonceHeader;

        try {
            this.sourceName = this.sqlTable.getConnection().getCatalog();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Failed to connect to local SQL database.", e);
        }

        this.selectStatement = initSelectStatement();
        try {
            resultSet = selectStatement.executeQuery();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Data could not be retrieved from the temporary database.", e);
        }
        refreshNextRow();
    }

    /**
     * Generates the SQL statement used in the PreparedStatement for retrieving all the rows of data.
     *
     * @param statement   A statement to be used purely for escaping column names
     * @param columnNames The columns to be selected
     * @param nonceHeader The column containing the nonce to order by
     * @return The SQL statement for selecting data
     * @throws C3rRuntimeException If columnNames or nonceHeader cannot be properly escaped
     */
    static String getSelectStatementSql(final Statement statement, final List<ColumnHeader> columnNames, final ColumnHeader nonceHeader) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        // Append all the escaped column names
        sb.append(columnNames.stream().map(column -> {
            try {
                return statement.enquoteIdentifier(column.toString(), true);
            } catch (SQLException e) {
                throw new C3rRuntimeException("Could not prepare internal statement for temporary database. Failed to escape column " +
                        "header: " + column, e);
            }
        }).collect(Collectors.joining(",")));
        try {
            final String nonce = statement.enquoteIdentifier(nonceHeader.toString(), true);
            sb.append(",").append(nonce).append(" FROM ").append(TableGenerator.DEFAULT_TABLE_NAME);
            sb.append(" ORDER BY ").append(nonce);
            return sb.toString();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Invalid SQL identifier encountered.", e);
        }
    }

    /**
     * Generates the PreparedStatement for selecting all the data in the table, ordered by the nonce column.
     *
     * @return A PreparedStatement ready for selecting data from the SQL table
     * @throws C3rRuntimeException If there's an error while setting up connection to SQL database
     */
    PreparedStatement initSelectStatement() {
        try {
            final Statement statement = sqlTable.getConnection().createStatement(); // Used strictly to escape column names
            final String sql = getSelectStatementSql(statement, headers, nonceHeader);
            return sqlTable.getConnection().prepareStatement(sql);
        } catch (SQLException e) {
            throw new C3rRuntimeException("Could not prepare internal statement for temporary database.", e);
        }
    }

    /**
     * Load the next row from the executed query into the private {@code nextRow} field.
     */
    @Override
    protected void refreshNextRow() {
        try {
            if (resultSet.next()) {
                nextRow = rowFactory.newRow();
                for (ColumnHeader column : headers) {
                    nextRow.putBytes(internalToTargetColumnHeaders.get(column), resultSet.getBytes(column.toString()));
                }
                nextRow.putNonce(nonceHeader, new Nonce(resultSet.getBytes(nonceHeader.toString())));
            } else {
                nextRow = null;
            }
        } catch (SQLException e) {
            throw new C3rRuntimeException("Data could not be retrieved from the temporary database.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row<T> peekNextRow() {
        return nextRow != null ? nextRow.clone() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            resultSet.close();
            selectStatement.close();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Could not close results from temporary database.", e);
        }
    }
}
