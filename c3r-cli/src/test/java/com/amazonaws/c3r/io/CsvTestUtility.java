// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
     * Creates a simple CSV parser for the specified columns that will read out {@code maxColumns}.
     *
     * @param fileName   Location of the file to read
     * @param maxColumns Maximum number of columns expected from file
     * @return Parser for getting file contents
     * @throws RuntimeException If the CSV file is not found
     */
    public static CsvParser getCsvParser(final String fileName, final Integer maxColumns) {
        try {
            final CsvParserSettings settings = getBasicParserSettings(maxColumns, false);

            // creates a CSV parser
            final CsvParser parser = new CsvParser(settings);
            final InputStreamReader reader = new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8);
            parser.beginParsing(reader);
            return parser;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create basic parser settings that don't modify/NULL any values
     * aside from the default whitespace trimming.
     *
     * @param maxColumns Most columns allowed in the CSV file
     * @param keepQuotes If quotes should be kept as part of the string read in or not
     * @return Settings to bring up a simple CSV parser
     */
    private static CsvParserSettings getBasicParserSettings(final Integer maxColumns, final boolean keepQuotes) {
        final CsvParserSettings settings = new CsvParserSettings();
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setNullValue("");
        settings.setEmptyValue("\"\"");
        settings.setKeepQuotes(keepQuotes);
        if (maxColumns != null) {
            settings.setMaxColumns(maxColumns);
        }

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
        final CsvParserSettings settings = getBasicParserSettings(null, true);
        settings.setHeaderExtractionEnabled(true);
        final CsvParser parser = new CsvParser(settings);
        return parser.parseAllRecords(new File(fileName)).stream().map(r -> r.toFieldMap()).collect(Collectors.toList());
    }

    /**
     * Read the file content with rows as arrays. There is no mapping to column headers, if any, in the file.
     *
     * @param fileName   Location of file to read
     * @param keepQuotes If quotes should be kept as part of the string read in or not
     * @return List of rows where each row is an array of values
     * @throws RuntimeException If the file is not found
     */
    public static List<String[]> readContentAsArrays(final String fileName, final boolean keepQuotes) {
        final CsvParserSettings settings = getBasicParserSettings(null, keepQuotes);
        return new CsvParser(settings).parseAll(new File(fileName), StandardCharsets.UTF_8);
    }
}
