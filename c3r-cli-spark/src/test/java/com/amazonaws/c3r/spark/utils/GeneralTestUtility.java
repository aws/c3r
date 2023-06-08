// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.encryption.keys.KeyUtil;

import javax.crypto.spec.SecretKeySpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Set of Utilities used for Testing. A combination of file settings and helper functions.
 */
public abstract class GeneralTestUtility {
    /**
     * A 32-byte key used for testing.
     */
    public static final byte[] EXAMPLE_KEY_BYTES =
            new byte[]{
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                    30, 31
            };

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
     * Schema for data_sample.csv.
     */
    public static final TableSchema CONFIG_SAMPLE = new MappedTableSchema(List.of(
            cleartextColumn("firstname"),
            cleartextColumn("lastname"),
            sealedColumn("address", PadType.MAX, 32),
            sealedColumn("city", PadType.MAX, 16),
            fingerprintColumn("state"),
            cleartextColumn("phonenumber", "phonenumber_cleartext"),
            sealedColumn("phonenumber", "phonenumber_sealed"),
            fingerprintColumn("phonenumber", "phonenumber_fingerprint"),
            sealedColumn("title", PadType.FIXED, 128),
            cleartextColumn("level"),
            sealedColumn("notes", PadType.MAX, 100)
    ));

    /**
     * Encryption configuration used for data_sample.csv (matches decryption configuration for marshalled_data_sample.csv).
     */
    public static final EncryptSdkConfigTestUtility TEST_CONFIG_DATA_SAMPLE = EncryptSdkConfigTestUtility.builder()
            .input("../samples/csv/data_sample_with_quotes.csv")
            .inputColumnHeaders(CONFIG_SAMPLE.getColumns().stream().map(ColumnSchema::getSourceHeader).map(ColumnHeader::toString)
                    .collect(Collectors.toList()))
            .outputColumnHeaders(CONFIG_SAMPLE.getColumns().stream().map(ColumnSchema::getTargetHeader).map(ColumnHeader::toString)
                    .collect(Collectors.toList()))
            .salt("saltybytes")
            .key(new SecretKeySpec(EXAMPLE_KEY_BYTES, KeyUtil.KEY_ALG))
            .schema(CONFIG_SAMPLE)
            .build();

    /**
     * Encryption configuration used for one_row_null_sample.csv with only cleartext columns.
     */
    public static final EncryptSdkConfigTestUtility TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT = EncryptSdkConfigTestUtility.builder()
            .input("../samples/csv/one_row_null_sample.csv")
            .inputColumnHeaders(List.of("firstname", "lastname", "address", "city"))
            .outputColumnHeaders(List.of("firstname", "lastname", "address", "city"))
            .salt("saltybytes")
            .key(new SecretKeySpec(EXAMPLE_KEY_BYTES, KeyUtil.KEY_ALG))
            .schema(new MappedTableSchema(Stream.of("firstname", "lastname", "address", "city").map(GeneralTestUtility::cleartextColumn)
                    .collect(Collectors.toList())))
            .build();

    /**
     * Create a ColumnHeader if name isn't null.
     *
     * <p>
     * This helper function is to support testing positional schemas. Those schemas need to have {@code null} as the value
     * for the sourceHeader. However, {@code new ColumnHeader(null)} fails validation. Instead of using the ternary operator
     * everywhere we assign the source value, we can call this function instead which is a bit cleaner. By having this helper,
     * we don't need to make another full set of helper functions for schema creation, we can just pass {@code null} in to the
     * existing helpers. {@link com.amazonaws.c3r.config.PositionalTableSchema} uses this functionality in the creation of all it's
     * test variables at the top of the file if you want to see an example usage of why we need to pass null through.
     *
     * @param name Name of the column or {@code null} if there isn't one
     * @return Input string transformed into {@link ColumnHeader} or {@code null} if {@code name} was {@code null}
     */
    private static ColumnHeader nameHelper(final String name) {
        if (name == null) {
            return null;
        }
        return new ColumnHeader(name);
    }

    /**
     * Helper function that handles cleartext column boilerplate.
     *
     * @param name Name to be used for input and output row
     * @return An cleartext column schema
     */
    public static ColumnSchema cleartextColumn(final String name) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(name))
                .targetHeader(nameHelper(name))
                .pad(null)
                .type(ColumnType.CLEARTEXT)
                .build();
    }

    /**
     * Helper function that handles cleartext column boilerplate.
     *
     * @param nameIn  Source column header name
     * @param nameOut Target column header name
     * @return An cleartext column schema
     */
    public static ColumnSchema cleartextColumn(final String nameIn, final String nameOut) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(nameIn))
                .targetHeader(nameHelper(nameOut))
                .pad(null)
                .type(ColumnType.CLEARTEXT)
                .build();
    }

    /**
     * Helper function for a sealed column with no pad.
     *
     * @param nameIn  Source header name
     * @param nameOut Target header name
     * @return A sealed column schema
     */
    public static ColumnSchema sealedColumn(final String nameIn, final String nameOut) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(nameIn))
                .targetHeader(nameHelper(nameOut))
                .pad(Pad.DEFAULT)
                .type(ColumnType.SEALED)
                .build();
    }

    /**
     * Helper function for a sealed column with specified padding.
     *
     * @param name   Name for source and target column headers
     * @param type   What pad type to use
     * @param length How long the pad should be
     * @return A sealed column schema
     */
    public static ColumnSchema sealedColumn(final String name, final PadType type, final Integer length) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(name))
                .targetHeader(nameHelper(name))
                .pad(Pad.builder().type(type).length(length).build())
                .type(ColumnType.SEALED)
                .build();
    }

    /**
     * Helper function for creating a fingerprint column.
     *
     * @param name The name to use for both the source and target header
     * @return A fingerprint column schema
     */
    public static ColumnSchema fingerprintColumn(final String name) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(name))
                .targetHeader(nameHelper(name))
                .type(ColumnType.FINGERPRINT)
                .build();
    }

    /**
     * Helper function for creating a fingerprint column.
     *
     * @param nameIn  The name to use for the source header
     * @param nameOut The name to use for the target header
     * @return A fingerprint column schema
     */
    public static ColumnSchema fingerprintColumn(final String nameIn, final String nameOut) {
        return ColumnSchema.builder()
                .sourceHeader(nameHelper(nameIn))
                .targetHeader(nameHelper(nameOut))
                .type(ColumnType.FINGERPRINT)
                .build();
    }

    /**
     * Build a simple Row from strings for testing; string values are used verbatim.
     *
     * @param rowEntries CSV row entries given in key, value, key, value, etc... order a la `Map.of(..)`
     * @return A row with the given key/value pairs
     */
    public static Map<String, String> row(final String... rowEntries) {
        final var row = new HashMap<String, String>();
        for (int i = 0; i < rowEntries.length; i += 2) {
            row.put(rowEntries[i], rowEntries[i + 1]);
        }
        return row;
    }

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
