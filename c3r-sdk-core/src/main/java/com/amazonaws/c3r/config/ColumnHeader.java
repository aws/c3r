// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.internal.Validatable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/**
 * Stores a normalized and validated column name (column header).
 */
@EqualsAndHashCode
public class ColumnHeader implements Validatable {
    /**
     * Default suffix for unspecified sealed target column names.
     */
    public static final String DEFAULT_SEALED_SUFFIX = "_sealed";

    /**
     * Default suffix for unspecified fingerprint column names.
     */
    public static final String DEFAULT_FINGERPRINT_SUFFIX = "_fingerprint";

    /**
     * The name of the Column.
     */
    private final String header;

    /**
     * Create a column header from the given name, normalizing it if necessary.
     *
     * @param header The name to use (possibly trimmed, made all lowercase)
     */
    public ColumnHeader(final String header) {
        this.header = normalize(header);
        validate();
    }

    /**
     * Creates a default target column header based off of source column header name and cryptographic primitive.
     *
     * @param sourceHeader Name of the source column
     * @param type         Type of cryptographic transform being applied
     * @return Default name for output column
     */
    private static ColumnHeader addDefaultColumnTypeSuffix(@NonNull final ColumnHeader sourceHeader, @NonNull final ColumnType type) {
        switch (type) {
            case SEALED:
                return new ColumnHeader(sourceHeader + DEFAULT_SEALED_SUFFIX);
            case FINGERPRINT:
                return new ColumnHeader(sourceHeader + DEFAULT_FINGERPRINT_SUFFIX);
            default:
                return sourceHeader;
        }
    }

    /**
     * Creates a default target column header based off of source column header name and cryptographic primitive if a specific header was
     * not provided.
     *
     * @param sourceHeader Name of the source column
     * @param targetHeader Name of the target header (if one was provided)
     * @param type         Type of cryptographic transform being applied
     * @return Default name for output column
     */
    public static ColumnHeader deriveTargetColumnHeader(final ColumnHeader sourceHeader,
                                                        final ColumnHeader targetHeader,
                                                        final ColumnType type) {
        if (sourceHeader != null && targetHeader == null && type != null) {
            return addDefaultColumnTypeSuffix(sourceHeader, type);
        } else {
            return targetHeader;
        }
    }

    /**
     * Construct the column name from a zero counted array. Column names are one counted.
     *
     * @param i Index of the column we want a name for
     * @return ColumnHeader where the name is "Column (i+1)"
     * @throws C3rIllegalArgumentException If the index is negative
     */
    public static ColumnHeader getColumnHeaderFromIndex(final int i) {
        if (i < 0) {
            throw new C3rIllegalArgumentException("Column index must be non-negative");
        }
        return new ColumnHeader("column " + (i + 1));
    }

    /**
     * Ensure all headers are turned into comparable strings by removing leading/trailing whitespace and making all headers lowercase.
     *
     * @param header Name to normalize
     * @return Trimmed and lowercase version of name
     */
    private String normalize(final String header) {
        if (header != null) {
            return header.trim().toLowerCase();
        }
        return null;
    }

    /**
     * Get the name this ColumnHeader represents as a String.
     *
     * @return Header name
     */
    @Override
    public String toString() {
        return header;
    }

    /**
     * Make sure the column header meets particular rules.
     * - The header must not be null or blank
     * - The length of the header must be short enough to be accepted by Glue
     * - The name must match the conventions set by Glue
     *
     * @throws C3rIllegalArgumentException If any of the rules are broken
     */
    public void validate() {
        if (header == null || header.isBlank()) {
            throw new C3rIllegalArgumentException("Column header names must not be blank");
        }
        if (header.length() > Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH) {
            throw new C3rIllegalArgumentException(
                    "Column header names cannot be longer than "
                            + Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH
                            + " characters, but found `"
                            + header
                            + "`.");
        }
        if (!Limits.AWS_CLEAN_ROOMS_HEADER_REGEXP.matcher(header).matches()) {
            throw new C3rIllegalArgumentException(
                    "Column header name `"
                            + header
                            + "` does not match pattern `"
                            + Limits.AWS_CLEAN_ROOMS_HEADER_REGEXP.pattern()
                            + "`.");
        }
    }
}
