// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_6COLUMN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Black-box testing for how the encryption client handles NULL entries in data.
public class RowMarshalPreserveNullTest {
    private String output;

    private EncryptConfig.EncryptConfigBuilder configBuilder;

    private ClientSettings.ClientSettingsBuilder settingsBuilder;

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.createTempFile("output", ".csv").toString();
        settingsBuilder = ClientSettings.builder()
                .allowCleartext(true)
                .allowDuplicates(false)
                .allowJoinsOnColumnsWithDifferentNames(false)
                .preserveNulls(false);

        configBuilder = EncryptConfig.builder()
                .overwrite(true)
                .salt("collaboration1234")
                .secretKey(new SecretKeySpec(GeneralTestUtility.EXAMPLE_KEY_BYTES, KeyUtil.KEY_ALG))
                .targetFile(output)
                .tempDir(FileUtil.TEMP_DIR)
                .settings(settingsBuilder.build());

    }

    private void validatePreserveNull(final String inputFile, final String inputNullValue, final String outputNullValue) {
        final EncryptConfig config = configBuilder
                .sourceFile(inputFile)
                .tableSchema(TEST_CONFIG_6COLUMN)
                .csvInputNullValue(inputNullValue)
                .csvOutputNullValue(outputNullValue)
                .settings(settingsBuilder.build())
                .build();

        final var rawNullString = Objects.requireNonNullElse(outputNullValue, "");

        final var marshaller = CsvRowMarshaller.newInstance(config);

        marshaller.marshal();
        marshaller.close();

        final List<String[]> encryptedRows = CsvTestUtility.readContentAsArrays(output, false);

        final String[] expectedHeaders =
                new String[]{"cleartext", "sealed_none", "sealed_max", "sealed_fixed", "fingerprint_1", "fingerprint_2"};
        assertArrayEquals(expectedHeaders, encryptedRows.get(0));

        // remove the header, make sure the rows are all still present
        encryptedRows.remove(0);
        assertEquals(5, encryptedRows.size());

        // check that the cleartext row is null, but the others are or are not
        // based on `preserveNulls`
        final Set<String> nonNullValues = new HashSet<>();
        for (String[] row : encryptedRows) {
            for (int j = 0; j < expectedHeaders.length; j++) {
                if (config.getSettings().isPreserveNulls() || j == 0) {
                    assertEquals(rawNullString, row[j]);
                } else {
                    assertNotEquals(rawNullString, row[j]);
                }
                if (!Objects.equals(row[j], rawNullString)) {
                    nonNullValues.add(row[j]);
                }
            }
        }

        // ensure we got the expected number of unique, non-null values
        if (config.getSettings().isPreserveNulls()) {
            // preserve nulls means everything is null and thus no non-null values!
            assertEquals(0, nonNullValues.size());
        } else {
            // if `preserveNULLs == false`
            // then there are 25 non-null entries (5 encrypted columns, 5 rows)
            assertEquals(25, nonNullValues.size());
        }
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesTrueTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/null5by6.csv", null, null);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesFalseTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/null5by6.csv", null, null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesTrueTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/null5by6.csv", null, null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesFalseTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/null5by6.csv", null, null);
    }

    @Test
    public void validatePreserveNullTrueAllowDuplicatesFalseTest() {
        configBuilder.sourceFile("../samples/csv/null5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        // if allowDuplicates = false and preserveNULLs = true,
        // NULL does not count as a value. So multiple NULL values are fine.
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowDuplicates(false);
        final var config = configBuilder.settings(settingsBuilder.build()).build();

        final var marshaller = CsvRowMarshaller.newInstance(config);
        assertDoesNotThrow(marshaller::marshal);
        marshaller.close();
    }

    @Test
    public void validatePreserveNullFalseAllowDuplicatesFalseTest() {
        configBuilder.sourceFile("../samples/csv/null5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowDuplicates(false);

        final var marshaller = CsvRowMarshaller.newInstance(configBuilder.build());
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesTrueCustomEmptyNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/null5by6.csv", "", null);
        validatePreserveNull("../samples/csv/empty5by6.csv", "\"\"", null);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesFalseCustomEmptyNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/null5by6.csv", "", null);
        validatePreserveNull("../samples/csv/empty5by6.csv", "\"\"", null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesTrueCustomEmptyNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/null5by6.csv", "", null);
        validatePreserveNull("../samples/csv/empty5by6.csv", "\"\"", null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesFalseCustomEmptyNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/null5by6.csv", "", null);
        validatePreserveNull("../samples/csv/empty5by6.csv", "\"\"", null);
    }

    @Test
    public void validatePreserveNullTrueAllowDuplicatesFalseCustomEmptyNullTest() {
        configBuilder.sourceFile("../samples/csv/null5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("");
        // if allowDuplicates = false and preserveNULLs = true,
        // NULL does not count as a value. So multiple NULL values are fine.
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowDuplicates(false);
        var config = configBuilder.settings(settingsBuilder.build()).build();

        var marshaller = CsvRowMarshaller.newInstance(config);
        assertDoesNotThrow(marshaller::marshal);
        marshaller.close();

        configBuilder.sourceFile("../samples/csv/empty5by6.csv").csvInputNullValue("\"\"");
        config = configBuilder.build();
        marshaller = CsvRowMarshaller.newInstance(config);
        assertDoesNotThrow(marshaller::marshal);
        marshaller.close();
    }

    @Test
    public void validatePreserveNullFalseAllowDuplicatesFalseCustomEmptyNullTest() {
        configBuilder.sourceFile("../samples/csv/null5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("");
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowDuplicates(false);

        final var marshaller = CsvRowMarshaller.newInstance(configBuilder.build());
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
        configBuilder.sourceFile("../samples/csv/empty5by6.csv");
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesTrueCustomNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", null);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesFalseCustomNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesTrueCustomNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", null);
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesFalseCustomNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", null);
    }

    @Test
    public void validatePreserveNullTrueAllowDuplicatesFalseCustomNullTest() {
        configBuilder.sourceFile("../samples/csv/customNull5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("null");
        // if allowDuplicates = false and preserveNULLs = true,
        // NULL does not count as a value. So multiple NULL values are fine.
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowDuplicates(false);
        final var config = configBuilder.settings(settingsBuilder.build()).build();

        final var marshaller = CsvRowMarshaller.newInstance(config);
        assertDoesNotThrow(marshaller::marshal);
        marshaller.close();
    }

    @Test
    public void validatePreserveNullFalseAllowDuplicatesFalseCustomNullTest() {
        configBuilder.sourceFile("../samples/csv/customNull5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("/null");
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowDuplicates(false);

        final var marshaller = CsvRowMarshaller.newInstance(configBuilder.build());
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesTrueCustomOutputNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", "null");
    }

    @Test
    public void validatePreserveNullTrueAllowJoinsOnColumnsWithDifferentNamesFalseCustomOutputNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", "null");
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesTrueCustomOutputNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(true);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", "null");
    }

    @Test
    public void validatePreserveNullFalseAllowJoinsOnColumnsWithDifferentNamesFalseCustomOutputNullTest() {
        settingsBuilder.allowDuplicates(true);
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowJoinsOnColumnsWithDifferentNames(false);
        validatePreserveNull("../samples/csv/customNull5by6.csv", "null", "null");
    }

    @Test
    public void validatePreserveNullTrueAllowDuplicatesFalseCustomOutputNullTest() {
        configBuilder.sourceFile("../samples/csv/customNull5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("null");
        configBuilder.csvOutputNullValue("null");
        // if allowDuplicates = false and preserveNULLs = true,
        // NULL does not count as a value. So multiple NULL values are fine.
        settingsBuilder.preserveNulls(true);
        settingsBuilder.allowDuplicates(false);
        final var config = configBuilder.settings(settingsBuilder.build()).build();

        final var marshaller = CsvRowMarshaller.newInstance(config);
        assertDoesNotThrow(marshaller::marshal);
        marshaller.close();
    }

    @Test
    public void validatePreserveNullFalseAllowDuplicatesFalseCustomOutputNullTest() {
        configBuilder.sourceFile("../samples/csv/customNull5by6.csv");
        configBuilder.tableSchema(TEST_CONFIG_6COLUMN);
        configBuilder.csvInputNullValue("null");
        configBuilder.csvOutputNullValue("null");
        settingsBuilder.preserveNulls(false);
        settingsBuilder.allowDuplicates(false);

        final var marshaller = CsvRowMarshaller.newInstance(configBuilder.build());
        assertThrows(C3rRuntimeException.class, marshaller::marshal);
    }
}
