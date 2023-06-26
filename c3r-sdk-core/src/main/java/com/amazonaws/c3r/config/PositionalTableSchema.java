// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class that implements {@link TableSchema} for data without named columns.
 * The length of the outer list must be the same as the number of columns in the data. The inner list is the various output columns each
 * input column should map to. If that column is not mapped to anything, the inner list can be empty or {@code null}. The outer list is
 * used in order of columns read, i.e., index 0 is the first column of data, index 1 is the second column of data, etc.
 */
@EqualsAndHashCode(callSuper = true)
public class PositionalTableSchema extends TableSchema {
    /**
     * The schema for each column created in the output file, as specified verbatim by the user.
     */
    private final List<List<ColumnSchema>> columns;

    /**
     * Columns updated to have a source default, positional column name.
     */
    private transient List<ColumnSchema> mappedColumns;

    /**
     * The names of columns in the file.
     */
    private transient List<ColumnHeader> sourceHeaders;

    /**
     * Construct a {@link TableSchema} and validates it for files without a header row.
     *
     * @param positionalColumns Specification for how each input column maps to a list of 0 or more columns that are in the output
     */
    public PositionalTableSchema(final List<List<ColumnSchema>> positionalColumns) {
        setHeaderRowFlag(false);
        columns = (positionalColumns == null) ? null : Collections.unmodifiableList(positionalColumns);
        validate();
    }

    /**
     * Generate positional source headers of the specified length.
     *
     * @param sourceColumnCount Number of positional headers
     * @return The list of positional headers in ascending order
     */
    public static List<ColumnHeader> generatePositionalSourceHeaders(final int sourceColumnCount) {
        return IntStream.range(0, sourceColumnCount)
                .mapToObj(ColumnHeader::of)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Take the positional column schemas and transform them into fully defined mapped column schemas for data processing.
     *
     * @return List of all specified columns
     * @throws C3rIllegalArgumentException If an invalid positional column schema is encountered
     */
    private List<ColumnSchema> mapPositionalColumns() {
        final List<ColumnSchema> localColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            final var columnI = columns.get(i);
            if (columnI != null && !columnI.isEmpty()) {
                for (ColumnSchema csJ : columnI) {
                    if (csJ == null) {
                        throw new C3rIllegalArgumentException("Invalid empty column specification found for column " + (i + 1));
                    }
                    localColumns.add(validateAndConfigureColumnSchema(i, csJ));
                }
            }
        }
        return Collections.unmodifiableList(localColumns);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ColumnSchema> getColumns() {
        if (mappedColumns == null && columns != null) {
            mappedColumns = mapPositionalColumns();
        }
        return mappedColumns != null ? new ArrayList<>(mappedColumns) : null;
    }

    /**
     * The names we are using for columns without a header value. "Column index" will be used.
     *
     * @return List of created column names
     */
    @Override
    public List<ColumnHeader> getPositionalColumnHeaders() {
        if (sourceHeaders == null) {
            sourceHeaders = generatePositionalSourceHeaders(columns.size());
        }
        return new ArrayList<>(sourceHeaders);
    }

    /**
     * Make sure specification matches requirements for a CSV file without a header row.
     *
     * @param sourceColumnIndex Index of column source content is derived from
     * @param column            Column to validate and finish filling out
     * @return Schema mapping an input column to an output column
     * @throws C3rIllegalArgumentException If the source header has a value or target header does not have a value
     */
    private ColumnSchema validateAndConfigureColumnSchema(final int sourceColumnIndex, final ColumnSchema column) {
        if (column.getSourceHeader() != null) {
            throw new C3rIllegalArgumentException("Positional table schemas cannot have `sourceHeader` properties in column schema, but " +
                    "found one in column " + (sourceColumnIndex + 1) + ".");
        }
        if (column.getTargetHeader() == null) {
            throw new C3rIllegalArgumentException("Positional table schemas must have a target header name for each column schema. " +
                    "Missing target header in column " + (sourceColumnIndex + 1) + ".");
        }
        return ColumnSchema.builder()
                .sourceHeader(ColumnHeader.of(sourceColumnIndex))
                .targetHeader(column.getTargetHeader())
                .pad(column.getPad())
                .type(column.getType())
                .build();
    }

    /**
     * Validates the final requirements of a positional schema (no header row in the data) and checks the rules for schemas overall.
     *
     * @throws C3rIllegalArgumentException If a rule for positional table schemas is not followed
     */
    @Override
    public void validate() {
        if (getHeaderRowFlag() != null && getHeaderRowFlag()) {
            throw new C3rIllegalArgumentException("Positional Table Schemas cannot use data containing a header row");
        }
        if (columns == null || columns.isEmpty()) {
            throw new C3rIllegalArgumentException("At least one data column must provided in the config file.");
        }
        if (mappedColumns == null) {
            mappedColumns = mapPositionalColumns();
        }
        if (sourceHeaders == null) {
            sourceHeaders = IntStream.range(0, columns.size()).mapToObj(ColumnHeader::of)
                    .collect(Collectors.toUnmodifiableList());
        }
        super.validate();
    }
}