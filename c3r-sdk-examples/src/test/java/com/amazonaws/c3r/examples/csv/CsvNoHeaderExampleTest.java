// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples.csv;

import com.amazonaws.c3r.examples.utils.FileTestUtility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvNoHeaderExampleTest {
    @Test
    public void roundTripTest() throws IOException {
        final Path inputCsv = Path.of("../samples/csv/data_sample_no_headers.csv");
        final Path encryptedCsv = FileTestUtility.createTempFile("encrypted", ".csv");
        final Path decryptedCsv = FileTestUtility.createTempFile("decrypted", ".csv");

        CsvNoHeaderExample.encrypt(inputCsv.toString(), encryptedCsv.toString());
        assertTrue(Files.size(encryptedCsv) > 0);

        CsvNoHeaderExample.decrypt(encryptedCsv.toString(), decryptedCsv.toString());

        assertTrue(Files.size(decryptedCsv) > 0);
    }
}
