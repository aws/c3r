// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.config.ColumnHeader;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Set of Utilities used for Testing. A combination of file settings and helper functions.
 */
public abstract class GeneralTestUtility {
    /**
     * Example salt for testing.
     */
    public static final UUID EXAMPLE_SALT = UUID.fromString("00000000-1111-2222-3333-444444444444");

    /**
     * List of headers from the golden test file (data_sample.csv).
     */
    public static final List<ColumnHeader> DATA_SAMPLE_HEADERS =
            List.of(new ColumnHeader("FirstName"),
                    new ColumnHeader("LastName"),
                    new ColumnHeader("Address"),
                    new ColumnHeader("City"),
                    new ColumnHeader("State"),
                    new ColumnHeader("PhoneNumber"),
                    new ColumnHeader("Title"),
                    new ColumnHeader("Level"),
                    new ColumnHeader("Notes"));

    /**
     * Takes a mapping of column headers to values along with a set of map entries for a column header to a test function.
     * This class creates the map of predicate functions by column header and calls {@link #assertRowEntryPredicates(Map, Map)}.
     *
     * @param content    A map of column headers to row content
     * @param predicates A variable length list of arguments that are map entries for testing row data
     * @see #assertRowEntryPredicates(Map, Map)
     */
    @SafeVarargs
    public static void assertRowEntryPredicates(final Map<String, String> content,
                                                final Map.Entry<String, Predicate<String>>... predicates) {
        assertRowEntryPredicates(content, Map.ofEntries(predicates));
    }

    /**
     * Using a mapping of headers to values and headers to test functions, verify each value in a row.
     *
     * @param content      Map of column headers to row content
     * @param predicateMap Map of column headers to a predicate function to check the column's value
     * @throws RuntimeException If the number of tests don't match the number of entries in the row
     */
    public static void assertRowEntryPredicates(final Map<String, String> content, final Map<String, Predicate<String>> predicateMap) {
        if (!content.keySet().equals(predicateMap.keySet())) {
            throw new RuntimeException(
                    String.join("\n",
                            "Bad test! Content keys and predicate keys don't match!",
                            "  Content headers: " + String.join(",", content.keySet()),
                            "Predicate headers: " + String.join(",", predicateMap.keySet())));
        }

        content.forEach((header, value) ->
                assertTrue(predicateMap.get(header).test(value),
                        "Row entry predicate failure: `" + header + "` -> `" + value + "`"));
    }
}
