// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvExampleTest {
    @Test
    public void roundTripTest() throws IOException {
        final Path inputCsv = Path.of("../samples/csv/data_sample_without_quotes.csv");
        final Path encryptedCsv = Files.createTempFile("encrypted", ".csv");
        final Path decryptedCsv = Files.createTempFile("decrypted", ".csv");

        com.amazonaws.c3r.examples.CsvExample.encrypt(inputCsv.toString(), encryptedCsv.toString());
        assertTrue(Files.exists(encryptedCsv));
        assertTrue(Files.size(encryptedCsv) > 0);

        com.amazonaws.c3r.examples.CsvExample.decrypt(encryptedCsv.toString(), decryptedCsv.toString());

        assertTrue(Files.exists(decryptedCsv));
        assertTrue(Files.size(decryptedCsv) > 0);
    }
}
