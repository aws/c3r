// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.internal.Validatable;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description of how columns of data in a CSV cleartext file map to the values in a CSV ciphertext file.
 */
@EqualsAndHashCode
public abstract class TableSchema implements Validatable {
    /**
     * Whether the data source has header values specified.
     *
     * <p>
     * Of note, this does need to be a {@code Boolean} and not a {@code boolean}. Since the latter has a default value of false,
     * it causes different error messages to be returned between {@code PositionalTableSchema} and {@code MappedTableSchema} when
     * the object isn't initialized properly from a JSON file. Different exception types are thrown from different points in the
     * code with {@code boolean} is used so {@code Boolean} provides a better user experience.
     */
    private Boolean headerRow;

    /**
     * Specifications for output columns.
     *
     * @return Descriptions for how each output column should be created
     */
    public abstract List<ColumnSchema> getColumns();

    /**
     * If an input file does not contain column headers, this function will return position-based column headers that
     * can be used in their place.
     *
     * @return Positional names to use for columns in an input file if applicable, else {@code null}
     */
    public abstract List<ColumnHeader> getPositionalColumnHeaders();

    /**
     * Determines if there's a need to run through the source file in order to ensure configuration constraints.
     * <p>
     * allowDuplicates set to false would require knowing if any data appears more than once to ensure the
     * restriction is met.
     * </p>
     *
     * @return {@code true} If there are any settings that require preprocessing
     */
    public boolean requiresPreprocessing() {
        return getColumns().stream().anyMatch(ColumnSchema::requiresPreprocessing);
    }

    /**
     * Check schema for valid configuration state.
     * <ul>
     *     <li>There must be at least one column</li>
     *     <li>There can't be more than the number of allowed columns in the output</li>
     *     <li>Each target header name can only be used once</li>
     * </ul>
     *
     * @throws C3rIllegalArgumentException If one of the rules is violated
     */
    @Override
    public void validate() {
        // Make sure we actually have a schema
        if (headerRow == null && getColumns() == null) {
            throw new C3rIllegalArgumentException("Schema was not initialized.");
        }

        // Check that headerRow is valid
        if (headerRow == null) {
            throw new C3rIllegalArgumentException("Schema must specify whether or not data has a header row.");
        }

        // Validate column information now that schema is complete
        final var columns = getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new C3rIllegalArgumentException("At least one data column must provided in the config file.");
        }
        if (columns.size() > Limits.ENCRYPTED_OUTPUT_COLUMN_COUNT_MAX) {
            throw new C3rIllegalArgumentException(
                    "An encrypted table can have at most "
                            + Limits.ENCRYPTED_OUTPUT_COLUMN_COUNT_MAX
                            + " columns "
                            + " but this schema specifies "
                            + getColumns().size()
                            + ".");
        }

        // Verify we have no duplicate target column headers
        // NOTE: target column headers must have already been normalized when checking for duplicates here
        // to ensure we don't get different column headers than end up being the same post-normalization.
        final Set<ColumnHeader> duplicateTargets = getColumns().stream()
                .collect(Collectors.groupingBy(ColumnSchema::getTargetHeader)).entrySet()
                .stream().filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (!duplicateTargets.isEmpty()) {
            final String duplicates = duplicateTargets.stream().map(ColumnHeader::toString)
                    .collect(Collectors.joining(", "));
            throw new C3rIllegalArgumentException("Target header name can only be used once. Duplicates found: " + duplicates);
        }
    }

    /**
     * The set of all column headers named in the schema (i.e., source and target).
     * If a source column name is used more than once or is reused as a target it will only be here once by definition of a set.
     *
     * @return Set of column names used in this schema
     */
    public Set<ColumnHeader> getSourceAndTargetHeaders() {
        return getColumns().stream()
                .flatMap(c -> Stream.of(c.getSourceHeader(), c.getTargetHeader()))
                .collect(Collectors.toSet());
    }

    /**
     * Set whether the table schema has a header row.
     *
     * @param hasHeaderRow {@code true} if the data has a header row
     */
    protected void setHeaderRowFlag(final boolean hasHeaderRow) {
        headerRow = hasHeaderRow;
    }

    /**
     * Get whether the table schema has a header row.
     *
     * @return {@code true} if the data has a header row
     */
    public Boolean getHeaderRowFlag() {
        return headerRow;
    }

    /**
     * Verifies that settings are consistent.
     * - If the clean room doesn't allow cleartext columns, verify none are in the schema
     *
     * @param schema   The TableSchema to validate
     * @param settings The ClientSettings to validate the TableSchema against
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    public static void validateSchemaAgainstClientSettings(final TableSchema schema, final ClientSettings settings) {
        if (!settings.isAllowCleartext()) {
            final Map<ColumnType, List<ColumnSchema>> typeMap = schema.getColumns().stream()
                    .collect(Collectors.groupingBy(ColumnSchema::getType));
            if (typeMap.containsKey(ColumnType.CLEARTEXT)) {
                final String targetColumns = typeMap.get(ColumnType.CLEARTEXT).stream()
                        .map(column -> column.getTargetHeader().toString())
                        .collect(Collectors.joining("`, `"));
                throw new C3rIllegalArgumentException(
                        "Cleartext columns found in the schema, but allowCleartext is false. Target " +
                                "column names: [`" + targetColumns + "`]");
            }
        }
    }
}
