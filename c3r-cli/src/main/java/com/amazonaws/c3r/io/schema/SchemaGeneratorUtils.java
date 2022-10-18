// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.config.ColumnHeader;

/**
 * Common utility functions used by schema generators.
 */
public final class SchemaGeneratorUtils {

    /** Hidden utility constructor. */
    private SchemaGeneratorUtils() {
    }

    /**
     * Returns a string for user-facing messages which references the specified column.
     * I.e., either {@code "column `COLUMN_NAME`"} or {@code "column COLUMN_1BASED_INDEX"}
     *
     * @param columnHeader The column header (if one exists)
     * @param columnIndex  The column's 0-based index
     * @return A reference string for user facing I/O
     */
    public static String columnReference(final ColumnHeader columnHeader, final int columnIndex) {
        if (columnHeader != null) {
            return "column `" + columnHeader + "`";
        } else {
            return ColumnHeader.getColumnHeaderFromIndex(columnIndex).toString();
        }
    }

    /**
     * Returns a user-facing warning message stating the specified column cannot be encrypted in any way.
     *
     * @param columnHeader The column header (if one exists)
     * @param columnIndex  The column's 0-based index
     * @return A warning string user facing I/O
     */
    public static String unsupportedTypeWarning(final ColumnHeader columnHeader, final int columnIndex) {
        final String columnName = columnReference(columnHeader, columnIndex);

        return "WARNING: " + columnName + " contains non-string data and cannot be\n" +
                "         used for cryptographic computing. Any target column(s) generated\n" +
                "         from this column will be cleartext.";
    }
}
