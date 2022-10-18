// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class represents the mapping of named input columns to output columns.
 * An example would be a CSV file with a header row or Parquet file.
 */
@EqualsAndHashCode(callSuper = true)
public class MappedTableSchema extends TableSchema {
    /**
     * Specifications for columns in the output file.
     */
    private final List<ColumnSchema> columns;

    /**
     * Validated specifications.
     */
    private transient List<ColumnSchema> validatedColumns;

    /**
     * Creates an instance of {@link TableSchema} for data files with header information.
     *
     * @param columnSchemas Specifications for how output columns should be created from input columns
     * @throws C3rIllegalArgumentException If schema doesn't contain at least one column
     */
    public MappedTableSchema(final List<ColumnSchema> columnSchemas) {
        setHeaderRowFlag(true);
        if (columnSchemas == null) {
            throw new C3rIllegalArgumentException("At least one data column must provided in the config file.");
        }
        columns = new ArrayList<>(columnSchemas);
        validate();
    }

    /**
     * Take the input columns schemas, verify they match mapped schema rules and modify target header if needed.
     *
     * @return Validated and completed schemas
     * @throws C3rIllegalArgumentException If source header is missing
     */
    private List<ColumnSchema> validateAndConfigureColumnSchemas() {
        final ArrayList<ColumnSchema> modifiedSchemas = new ArrayList<>(columns.size());
        for (ColumnSchema cs : columns) {
            if (cs.getSourceHeader() == null) {
                throw new C3rIllegalArgumentException("Source header is required.");
            }
            final var targetHeader = ColumnHeader.deriveTargetColumnHeader(cs.getSourceHeader(), cs.getTargetHeader(), cs.getType());
            modifiedSchemas.add(ColumnSchema.builder()
                    .sourceHeader(cs.getSourceHeader())
                    .targetHeader(targetHeader)
                    .internalHeader(cs.getInternalHeader())
                    .pad(cs.getPad())
                    .type(cs.getType())
                    .build());
        }
        return Collections.unmodifiableList(modifiedSchemas);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ColumnSchema> getColumns() {
        if (validatedColumns == null) {
            validatedColumns = validateAndConfigureColumnSchemas();
        }
        return new ArrayList<>(validatedColumns);
    }

    /**
     * MappedTableSchemas do not autogenerate any header names so {@code null} is always returned.
     *
     * @return {@code null}
     */
    @Override
    public List<ColumnHeader> getPositionalColumnHeaders() {
        return null;
    }

    /**
     * Validates the remaining requirement that mapped table schemas must have a header row and requirements for schemas overall.
     *
     * @throws C3rIllegalArgumentException If a rule for mapped table schemas is not followed
     */
    @Override
    public void validate() {
        if (getHeaderRowFlag() == null || !getHeaderRowFlag()) {
            throw new C3rIllegalArgumentException("Mapped Table Schemas require a header row in the data.");
        }
        if (getPositionalColumnHeaders() != null) {
            throw new C3rIllegalArgumentException("Mapped schemas should not have any unspecified input headers.");
        }
        if (validatedColumns == null) {
            validatedColumns = validateAndConfigureColumnSchemas();
        }
        super.validate();
    }
}
