// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.sql;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * Creates a SQL table database file.
 */
public abstract class TableGenerator {
    /**
     * Default name for database file.
     */
    public static final String DEFAULT_TABLE_NAME = "c3rTmp";

    /**
     * Creates the temporary SQL table used for data processing. If the table is found to already exist, it is deleted before being
     * recreated.
     *
     * @param schema      User-provided schema for table to be generated
     * @param nonceHeader The column name used for storing nonces
     * @param tempDir     The directory where the temporary SQL table will be created
     * @return A Connection to the table
     * @throws C3rRuntimeException If the temporary database file couldn't be created
     */
    public static SqlTable initTable(final TableSchema schema, final ColumnHeader nonceHeader,
                                     final String tempDir) {
        // Clean up tmp table if it existed already
        final File tempDbFile = initTableFile(tempDir);

        try {
            final SqlTable sqlTable = new SqlTable(tempDbFile);
            final Statement stmt = sqlTable.getConnection().createStatement();
            final String sql = getTableSchemaFromConfig(stmt, schema, nonceHeader);
            stmt.execute(sql);

            // Disable journaling to avoid leaving journal files on disk if there is an exception during execution,
            // and set synchronous to OFF for performance.
            // Neither should be an issue since our table is ephemeral and is not intended to live or be
            // used outside a single program execution.
            stmt.execute("PRAGMA synchronous = OFF;");
            stmt.execute("PRAGMA journal_mode = OFF;");
            stmt.close();

            return sqlTable;
        } catch (SQLException e) {
            throw new C3rRuntimeException("The temporary database used for processing could not be created. File: "
                    + tempDbFile.getAbsolutePath(), e);
        }
    }

    /**
     * Creates a temporary SQL table file used for data processing.
     *
     * @param tempDir The directory used for the temporary SQL table
     * @return Temporary system file to use as database file
     * @throws C3rRuntimeException If the database file could not be created
     */
    static File initTableFile(@NonNull final String tempDir) {
        try {
            final File tempDbFile = File.createTempFile("c3rTmp", ".db", new File(tempDir));
            // Set 600 file permissions
            FileUtil.setOwnerReadWriteOnlyPermissions(tempDbFile);
            // Ensure file is cleaned up when JVM is closed.
            tempDbFile.deleteOnExit();
            return tempDbFile;
        } catch (InvalidPathException | IOException e) {
            throw new C3rRuntimeException("The temporary database used for processing could not be created in the temp directory. " +
                    "Directory: " + tempDir, e);
        }
    }

    /**
     * Generates the SQL for creating a table based on the columns that were provided in the TableSchema.
     *
     * @param stmt        The Statement from the Connection allowing all the columns to be properly escaped if necessary
     * @param schema      The schema used to create the SQL table
     * @param nonceHeader The column name for storing nonces
     * @return A SQL string for creating the dynamic table
     * @throws C3rRuntimeException If SQL table could not be created
     */
    static String getTableSchemaFromConfig(final Statement stmt, final TableSchema schema,
                                           final ColumnHeader nonceHeader) {
        try {
            final StringBuilder sb = new StringBuilder();
            // NOTE: we do not declare the nonce column to be a PRIMARY KEY up front to increase performance
            // for large numbers of inserts, and instead we later make a UNIQUE INDEX on the nonce
            // _after_ all the data is loaded. (See `getIndexStatement` in this file).
            sb.append("CREATE TABLE ").append(DEFAULT_TABLE_NAME)
                    .append(" (\n")
                    .append(stmt.enquoteIdentifier(nonceHeader.toString(), false))
                    .append(" TEXT");
            for (ColumnSchema columnSchema : schema.getColumns()) {
                sb.append(",\n").append(stmt.enquoteIdentifier(columnSchema.getInternalHeader().toString(), true)).append(" TEXT");
            }
            sb.append(")");
            return sb.toString();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Invalid SQL identifier encountered.", e);
        }
    }

    /**
     * Generate the SQL for querying a column for duplicate non-null entries.
     *
     * @param stmt         SQL Statement for identifier formatting
     * @param columnHeader Header for column to check for duplicates
     * @return The string-encoded SQL query
     * @throws C3rRuntimeException If an error occurs formatting the column header name
     */
    public static String getDuplicatesInColumnStatement(final Statement stmt, final ColumnHeader columnHeader) {
        try {
            final StringBuilder sb = new StringBuilder();
            final String quotedColumnHeader = stmt.enquoteIdentifier(columnHeader.toString(), true);
            sb.append("SELECT ")
                    .append(quotedColumnHeader)
                    .append(" FROM ")
                    .append(stmt.enquoteIdentifier(TableGenerator.DEFAULT_TABLE_NAME, true))
                    .append(" GROUP BY ")
                    .append(quotedColumnHeader)
                    .append(" HAVING COUNT(")
                    .append(quotedColumnHeader) // using column name here instead of `*` excludes NULL entries
                    .append(") > 1");
            return sb.toString();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Invalid SQL identifier encountered.", e);
        }
    }

    /**
     * Generate a fresh/unused header name.
     *
     * @param usedNames {@code ColumnHeader}s already in use
     * @param nameBase  Name to use if available. Serves as base for freshly generated name if already in use
     * @return A unique header name
     */
    public static ColumnHeader generateUniqueHeader(@NonNull final Set<ColumnHeader> usedNames, @NonNull final String nameBase) {
        final ColumnHeader defaultNonceHeader = new ColumnHeader(nameBase);
        // Generate a unique name for the nonce column that does not clash with any other column names,
        // erring on the side of caution by avoiding both source and target headers (so we are guaranteed
        // uniqueness regardless of how data is transformed).

        // guarantee nonceHeader is unique
        ColumnHeader nonceHeader = defaultNonceHeader;
        int n = 0;
        while (usedNames.contains(nonceHeader)) {
            nonceHeader = new ColumnHeader(defaultNonceHeader.toString() + n);
            n++;
        }
        return nonceHeader;
    }

    /**
     * Generate an SQL statement to create a covering UNIQUE INDEX for each column, leading with the nonce on the left,
     * so it is the primary key of the index.
     *
     * @param stmt        Statement for escaping identifiers
     * @param schema      Original table schema (used to guarantee freshness of index name)
     * @param nonceHeader Which column has the nonces
     * @return A `CREATE UNIQUE INDEX ...` statement for the nonce column
     * @throws C3rRuntimeException If an error is encountered using the Statement to enquote IDs
     */
    public static String getCoveringIndexStatement(final Statement stmt, final TableSchema schema, final ColumnHeader nonceHeader) {
        final Set<ColumnHeader> usedHeaders = schema.getSourceAndTargetHeaders();
        usedHeaders.add(nonceHeader);
        final ColumnHeader nonceIndexHeader = TableGenerator.generateUniqueHeader(usedHeaders, "row_nonce_idx");

        try {
            // NOTE: We lead with the nonce column on the left-most side of the index, since per
            // https://www.sqlite.org/queryplanner.html "The left-most column is the primary key used for ordering
            // the rows in the index."
            final StringBuilder sb = new StringBuilder();
            sb.append("CREATE UNIQUE INDEX ")
                    .append(stmt.enquoteIdentifier(nonceIndexHeader.toString(), true))
                    .append(" ON ")
                    .append(stmt.enquoteIdentifier(TableGenerator.DEFAULT_TABLE_NAME, true))
                    .append("(")
                    .append(stmt.enquoteIdentifier(nonceHeader.toString(), true));
            // We include the other columns in the index since (a) it does not affect the ordering (the nonce is
            // both unique and leftmost, so the other columns will never affect the ordering) and (b) this
            // increases the performance of fetching the entire rows in order, since all the content is
            // co-located in the index and fewer database fetches have to be performed than when _only_
            // indexing on the nonce.
            for (ColumnSchema columnSchema : schema.getColumns()) {
                sb.append(", ").append(stmt.enquoteIdentifier(columnSchema.getTargetHeader().toString(), true));
            }
            sb.append(");");
            return sb.toString();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Invalid SQL identifier encountered.", e);
        }
    }
}
