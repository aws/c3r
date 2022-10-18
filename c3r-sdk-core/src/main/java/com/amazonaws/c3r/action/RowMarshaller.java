// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.RowFactory;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.ColumnInsight;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.io.RowReader;
import com.amazonaws.c3r.io.RowWriter;
import com.amazonaws.c3r.io.SqlRowReader;
import com.amazonaws.c3r.io.SqlRowWriter;
import com.amazonaws.c3r.io.sql.SqlTable;
import com.amazonaws.c3r.io.sql.TableGenerator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interface for loading and processing records for encryption.
 *
 * @param <T> Data format
 * @see RowReader
 * @see RowWriter
 */
@Slf4j
public final class RowMarshaller<T extends Value> {
    /**
     * The number of rows to be read in at a time in order to help protect any columns marked with {@link PadType#MAX} on first pass.
     **/
    private static final int ROW_BATCH_SIZE = 100;

    /**
     * Number of inserts into the SQL database should be performed before committing the transaction.
     */
    static final int INSERTS_PER_COMMIT = 100 * ROW_BATCH_SIZE;

    /**
     * Number of rows to process before logging an update on progress.
     */
    static final int LOG_ROW_UPDATE_FREQUENCY = 10 * INSERTS_PER_COMMIT;

    /**
     * Cryptographic settings for the clean room.
     */
    private final ClientSettings settings;

    /**
     * Name of nonce column.
     */
    @Getter
    private final ColumnHeader nonceHeader;

    /**
     * Stores information about data processed so far for each input column.
     */
    private final Map<ColumnHeader, List<ColumnInsight>> sourceMappedColumnInsights;

    /**
     * Stores information about data processed so far for each output column.
     */
    private final Map<ColumnHeader, ColumnInsight> targetMappedColumnInsights;

    /**
     * Underlying storage location for information about processed data so {@code sourceMappedColumnInsights} and
     * {@code targetMappedColumnInsights} don't duplicate information or get out of sync.
     */
    @Getter
    private final Collection<ColumnInsight> columnInsights;

    /**
     * Used for reading a row of data.
     */
    @Getter
    private final RowReader<T> inputReader;

    /**
     * Used for writing a row of data.
     */
    @Getter
    private final RowWriter<T> outputWriter;

    /**
     * SQL table used for intermediate processing of data when two passes need to be done across the data.
     */
    @Getter
    private final SqlTable sqlTable;

    /**
     * Creates an empty row to be filled with data.
     */
    private final RowFactory<T> rowFactory;

    /**
     * Cryptographic computation transformers in use for data processing.
     */
    private final Map<ColumnType, Transformer> transformers;

    /**
     * Description of how input columns are transformed into output columns.
     */
    private final TableSchema schema;

    /**
     * Creates a {@code RowMarshaller} for encrypting data. See {@link #marshal} for primary functionality.
     *
     * @param settings     User-provided clean room settings
     * @param schema       User-provided table-specific schema
     * @param rowFactory   Creates new rows for the marshalled data
     * @param inputReader  Where records are derived from
     * @param outputWriter Where records are stored after being marshalled
     * @param tempDir      Location where temp files may be created while processing the input
     * @param transformers Transformers for each type of column
     */
    @Builder(access = AccessLevel.PACKAGE)
    private RowMarshaller(@NonNull final ClientSettings settings,
                          @NonNull final TableSchema schema,
                          @NonNull final RowFactory<T> rowFactory,
                          @NonNull final RowReader<T> inputReader,
                          @NonNull final RowWriter<T> outputWriter,
                          @NonNull final String tempDir,
                          @NonNull final Map<ColumnType, Transformer> transformers) {
        this.settings = settings;
        this.columnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        this.sourceMappedColumnInsights = this.columnInsights.stream()
                .collect(Collectors.groupingBy(ColumnSchema::getSourceHeader));
        this.targetMappedColumnInsights = this.columnInsights.stream()
                .collect(Collectors.toMap(ColumnSchema::getTargetHeader, Function.identity()));
        this.inputReader = inputReader;
        this.nonceHeader = TableGenerator.generateUniqueHeader(schema.getSourceAndTargetHeaders(), "row_nonce");
        this.rowFactory = rowFactory;
        this.outputWriter = outputWriter;
        populateColumnSpecPositions();
        this.sqlTable = TableGenerator.initTable(schema, this.nonceHeader, tempDir);
        this.transformers = Collections.unmodifiableMap(transformers);
        this.schema = schema;
        validate();
    }

    /**
     * Update {@link ColumnSchema}s with their source and target column positions.
     *
     * <p>
     * Missing columns will cause an error to be thrown.
     */
    private void populateColumnSpecPositions() {
        // Maps source column headers to location in data file
        final Map<ColumnHeader, Integer> columnPositions = new LinkedHashMap<>();
        final List<ColumnHeader> headers = inputReader.getHeaders();

        for (int i = 0; i < headers.size(); i++) {
            columnPositions.put(headers.get(i), i);
        }

        columnInsights.forEach(ci -> {
            if (columnPositions.containsKey(ci.getSourceHeader())) {
                ci.setSourceColumnPosition(columnPositions.get(ci.getSourceHeader()));
            }
        });
    }

    /**
     * Reads in records/rows from the given source and outputs it to the given target after applying cryptographic transforms.
     */
    public void marshal() {
        loadInput();
        marshalOutput();
    }

    /**
     * Check to see if more than one {@code null} value was seen and whether that should produce an error.
     *
     * @param insight Information about the data in a particular column
     * @param value   Check to see if this value is {@code null} and if it violates clean room settings
     * @throws C3rRuntimeException If more than one {@code null} entry was found and repeated fingerprint values are not allowed
     */
    private void checkForInvalidNullDuplicates(final ColumnInsight insight, final Value value) {
        // Manually check for disallowed NULL duplicates because SQLite and many other
        // database engines consider NULLs distinct in a UNIQUE column.
        if (!settings.isAllowDuplicates()
                && !settings.isPreserveNulls()
                && insight.getType() == ColumnType.FINGERPRINT
                && value.isNull()
                && insight.hasSeenNull()) {
            throw new C3rRuntimeException("Source column " + (insight.getSourceColumnPosition() + 1)
                    + " cannot be used to construct the target fingerprint column `" + insight.getTargetHeader().toString() + "` because"
                    + " the column contains more than one NULL entry"
                    + " and the `allowDuplicates` setting is false.");
        }
    }

    /**
     * Reads in records/rows from the given source.
     *
     * @throws C3rRuntimeException If there's an error accessing the SQL database
     */
    void loadInput() {
        try {
            log.debug("Loading data from {}.", inputReader.getSourceName());
            long commitFuel = INSERTS_PER_COMMIT;
            final long startTime = System.currentTimeMillis();
            final RowWriter<T> sqlRowWriter = new SqlRowWriter<>(columnInsights, nonceHeader, sqlTable);
            List<Row<T>> batchedRows = new ArrayList<>();

            // For bulk operations, we want to explicitly commit the transaction less often for performance.
            sqlTable.getConnection().setAutoCommit(false);
            while (inputReader.hasNext()) {
                final Row<T> sourceRow = inputReader.next();
                sourceRow.forEach((column, value) -> {
                    // Observe and validate current row
                    if (sourceMappedColumnInsights.containsKey(column)) {
                        for (var columnInsight : sourceMappedColumnInsights.get(column)) {
                            checkForInvalidNullDuplicates(columnInsight, value);
                            columnInsight.observe(value);
                        }
                    }
                });

                batchedRows.add(sourceRow);

                // If batch size or end of input is met, write to SQL and reset batch.
                if (batchedRows.size() == ROW_BATCH_SIZE || !inputReader.hasNext()) {
                    writeInputBatchToSql(sqlRowWriter, batchedRows);
                    commitFuel = commitFuel - batchedRows.size();
                    batchedRows = new ArrayList<>();

                    if (commitFuel <= 0) {
                        sqlTable.getConnection().commit();
                        commitFuel = INSERTS_PER_COMMIT;
                    }
                }

                if (inputReader.getReadRowCount() % LOG_ROW_UPDATE_FREQUENCY == 0) {
                    log.info("{} rows loaded.", inputReader.getReadRowCount());
                }
            }
            sqlTable.getConnection().commit();
            // We've completed our bulk insert, so turn autocommit back on
            // so any future one-off commands execute immediately.
            sqlTable.getConnection().setAutoCommit(true);
            final long endTime = System.currentTimeMillis();
            log.debug("Done loading {} rows in {} seconds.", inputReader.getReadRowCount(),
                    TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));

            checkForInvalidDuplicates();
        } catch (SQLException e) {
            throw new C3rRuntimeException("Error accessing the SQL database.", e);
        }
    }

    /**
     * Writes a batch of rows to SQL.
     *
     * @param sqlRowWriter The writer for the SQL database
     * @param batchedRows  The rows to be written to the database
     * @throws C3rRuntimeException If the transform couldn't be applied to a value or while writing to the database
     */
    private void writeInputBatchToSql(final RowWriter<T> sqlRowWriter, final List<Row<T>> batchedRows) {
        for (Row<T> sourceRow : batchedRows) {
            final Row<T> targetRow = rowFactory.newRow();
            final Nonce nonce = Nonce.nextNonce();
            sourceRow.forEach((column, value) -> {
                // Map source values to each target.
                if (sourceMappedColumnInsights.containsKey(column)) {
                    for (var columnInsight : sourceMappedColumnInsights.get(column)) {
                        // Marshal sensitive data. Note that PadType.MAX may not be correct at this stage. It will require decrypting and
                        // re-encrypting when being sent to the final output. In the interim, it is based on the running max byte length,
                        // sampled in batches.
                        final Transformer transformer = transformers.get(columnInsight.getType());
                        final byte[] bytesToMarshall = value.getBytes();

                        final var encryptionContext = new EncryptionContext(columnInsight, nonce, value.getClientDataType());

                        try {
                            targetRow.putBytes(
                                    columnInsight.getTargetHeader(),
                                    transformer.marshal(bytesToMarshall, encryptionContext));
                        } catch (Exception e) {
                            throw new C3rRuntimeException("Failed while marshalling data for target column `"
                                    + encryptionContext.getColumnLabel() + "` on row " + inputReader.getReadRowCount() + ". Error message: "
                                    + e.getMessage(), e);
                        }
                    }
                }
            });
            targetRow.putNonce(nonceHeader, nonce);
            try {
                sqlRowWriter.writeRow(targetRow);
            } catch (Exception e) {
                throw new C3rRuntimeException("Failed while marshalling data for row " + inputReader.getReadRowCount() + ". Error message: "
                        + e.getMessage(), e);
            }
        }
    }

    /**
     * Check the SQL database for duplicate values not allowed by client settings.
     *
     * @throws C3rRuntimeException If there are SQL exceptions or if invalid duplicates are found
     */
    private void checkForInvalidDuplicates() {
        final List<ColumnHeader> fingerprintColumns = columnInsights.stream()
                .filter(ci -> ci.getType() == ColumnType.FINGERPRINT)
                .map(ColumnInsight::getInternalHeader)
                .collect(Collectors.toList());

        if (settings.isAllowDuplicates() || fingerprintColumns.isEmpty()) {
            return;
        }

        log.debug("Checking for duplicates in {} {} columns.", fingerprintColumns.size(), ColumnType.FINGERPRINT);
        final long startTime = System.currentTimeMillis();
        final List<String> columnsWithDuplicates = new ArrayList<>();

        try (Statement stmt = this.sqlTable.getConnection().createStatement()) {
            for (var columnHeader : fingerprintColumns) {
                final ResultSet duplicates = stmt.executeQuery(TableGenerator.getDuplicatesInColumnStatement(stmt, columnHeader));
                if (duplicates.next()) {
                    columnsWithDuplicates.add(columnHeader.toString());
                }
                duplicates.close();
            }
        } catch (SQLException e) {
            throw new C3rRuntimeException("An SQL exception occurred during marshalling.", e);
        }
        final long endTime = System.currentTimeMillis();
        log.debug("Finished checking for duplicates in {} {} columns in {} seconds.", fingerprintColumns.size(), ColumnType.FINGERPRINT,
                TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));

        if (!columnsWithDuplicates.isEmpty()) {
            throw new C3rRuntimeException("Duplicate entries found in the following " + ColumnType.FINGERPRINT + " columns "
                    + "but the allowDuplicates setting for cryptographic computing is false: "
                    + "[" + String.join(", ", columnsWithDuplicates) + "]");
        }
    }

    /**
     * Writes out records/rows to the given target. Must be called after {@link #loadInput()} or no data will be output.
     *
     * @throws C3rRuntimeException If an error is encountered while using the SQL database
     */
    void marshalOutput() {
        log.debug("Randomizing data order.");
        long startTime = System.currentTimeMillis();

        // Create a covering index for all rows to improve our ORDER BY performance
        // to sort the table by nonce and induce a random order.
        try {
            final var stmt = this.sqlTable.getConnection().createStatement();
            stmt.execute(TableGenerator.getCoveringIndexStatement(stmt, schema, nonceHeader));
        } catch (SQLException e) {
            throw new C3rRuntimeException("An SQL exception occurred during marshalling.", e);
        }
        long endTime = System.currentTimeMillis();
        log.debug("Done randomizing data order in {} seconds.", TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));

        log.debug("Emitting encrypted data.");
        startTime = System.currentTimeMillis();

        final RowReader<T> sqlRowReader = new SqlRowReader<>(columnInsights, nonceHeader, rowFactory, sqlTable);
        while (sqlRowReader.hasNext()) {
            final Row<T> rowOut = sqlRowReader.next();
            final Row<T> marshalledRow = rowFactory.newRow();
            final Nonce nonce = new Nonce(rowOut.getValue(nonceHeader).getBytes());
            // Nonces don't get written to final output
            rowOut.removeColumn(nonceHeader);
            rowOut.forEach((column, value) -> {
                final var columnInsight = targetMappedColumnInsights.get(column);
                final Transformer transformer = transformers.get(columnInsight.getType());
                byte[] marshalledBytes = value.getBytes();

                // Replace bytes for columns marked with PadType.MAX now that we know the longest value length.
                // All other values are already marshalled correctly.
                if (columnInsight.getPad() != null && columnInsight.getPad().getType() == PadType.MAX) {
                    final EncryptionContext encryptionContext = new EncryptionContext(columnInsight, nonce, value.getClientDataType());
                    final byte[] unmarshalledMaxColumnBytes = transformer.unmarshal(marshalledBytes);
                    marshalledBytes = transformer.marshal(unmarshalledMaxColumnBytes, encryptionContext);
                }

                marshalledRow.putBytes(column, marshalledBytes);
            });
            outputWriter.writeRow(marshalledRow);
            if (sqlRowReader.getReadRowCount() % LOG_ROW_UPDATE_FREQUENCY == 0) {
                log.info("{} rows emitted.", sqlRowReader.getReadRowCount());
            }
        }
        outputWriter.flush();
        endTime = System.currentTimeMillis();
        log.debug("Done emitting {} encrypted rows in {} seconds.", sqlRowReader.getReadRowCount(),
                TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));
    }

    /**
     * Closes the connections to the input source, output target and SQL database as well as deleting the database.
     *
     * @throws C3rRuntimeException  If there's an error closing connections to the input file, output file, SQL database,
     *                              or the SQL database file.
     */
    public void close() {
        try {
            inputReader.close();
            outputWriter.close();
            if (sqlTable.getConnection() != null && !sqlTable.getConnection().isClosed()) {
                sqlTable.getConnection().close();
            }
            Files.delete(sqlTable.getDatabaseFile().toPath());
        } catch (IOException e) {
            throw new C3rRuntimeException("Unable to close file connection.", e);
        } catch (SQLException e) {
            throw new C3rRuntimeException("Access error while attempting to close the SQL database.", e);
        }
    }

    /**
     * Looks for any columns in the schema that are missing from the file.
     *
     * @throws C3rRuntimeException If specified input columns are missing from the file
     */
    private void validate() {
        final Set<String> missingHeaders = columnInsights.stream()
                .filter(ci -> ci.getSourceColumnPosition() < 0)
                .map(column -> column.getTargetHeader().toString())
                .collect(Collectors.toSet());

        if (!missingHeaders.isEmpty()) {
            throw new C3rRuntimeException("Target column(s) ["
                    + String.join(", ", missingHeaders)
                    + "] could not be matched to the corresponding source columns in the input file.");
        }
    }
}
