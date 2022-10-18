// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility functions for common CSV data manipulation needed during testing.
 */
public final class CsvTestUtility {
    /**
     * Hidden utility class constructor.
     */
    private CsvTestUtility() {
    }

    /**
     * Create basic parser settings that don't modify/NULL any values
     * aside from the default whitespace trimming.
     *
     * @param keepQuotes If quotes should be kept as part of the string being read in or not
     * @return Settings to bring up a simple CSV parser
     */
    private static CsvParserSettings getBasicParserSettings(final boolean keepQuotes) {
        final CsvParserSettings settings = new CsvParserSettings();
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setNullValue("");
        settings.setEmptyValue("\"\"");
        settings.setKeepQuotes(keepQuotes);

        return settings;
    }

    /**
     * Read the contents of the CSV file as rows, mapping column names to content.
     *
     * <p>
     * The column names are normalized per the C3R's normalizing (lower-cased and whitespace trimmed).
     *
     * @param fileName File to read
     * @return Rows read in the order they appear
     * @throws C3rIllegalArgumentException If the file does not have the same number of entries in each row
     */
    public static List<Map<String, String>> readRows(final String fileName) {
        final List<String[]> contents = readContentAsArrays(fileName, true);

        final String[] headers = contents.get(0);
        for (int i = 0; i < headers.length; i++) {
            headers[i] = new ColumnHeader(headers[i]).toString();
        }
        contents.remove(0);

        final var rows = new ArrayList<Map<String, String>>();

        for (String[] rawRow : contents) {
            if (rawRow.length != headers.length) {
                throw new C3rIllegalArgumentException("CSV file had inconsistent header and content count!");
            }
            final var row = new HashMap<String, String>();
            for (int i = 0; i < rawRow.length; i++) {
                row.put(headers[i], rawRow[i]);
            }
            rows.add(row);
        }

        return rows;
    }

    /**
     * Read the file content with rows as arrays. There is no mapping to column headers, if any, in the file.
     *
     * @param fileName   Location of file to read
     * @param keepQuotes If quotes should be kept as part of the string being read in or not
     * @return List of rows where each row is an array of values
     * @throws RuntimeException If the file is not found
     */
    public static List<String[]> readContentAsArrays(final String fileName, final boolean keepQuotes) {
        try {
            final CsvParserSettings settings = getBasicParserSettings(keepQuotes);

            final CsvParser parser = new CsvParser(settings);
            final InputStreamReader reader = new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8);
            return parser.parseAll(reader);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compares two CSV values for equality, ignoring quote marks if {@code ignoreQuotes} is true.
     *
     * @param val1 First CSV value to compare
     * @param val2 Second CSV value to compare
     * @return {@code true} if the values are equivalent
     */
    public static boolean compareCsvValues(final String val1, final String val2) {
        if (val1 == null && val2 == null) {
            return true;
        } else if (val1 == null || val2 == null) {
            return false;
        }
        return val1.compareTo(val2) == 0;
    }
}
