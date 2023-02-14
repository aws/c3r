// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples.parquet;

import com.amazonaws.c3r.examples.utils.FileTestUtility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetExampleTest {

    @Test
    public void roundTripTest() throws IOException {
        final Path inputParquet = Path.of("../samples/parquet/data_sample.parquet");
        final Path encryptedParquet = FileTestUtility.createTempFile("encrypted", ".parquet");
        final Path decryptedParquet = FileTestUtility.createTempFile("decrypted", ".parquet");

        ParquetExample.encrypt(inputParquet.toString(), encryptedParquet.toString());
        assertTrue(Files.size(encryptedParquet) > 0);

        ParquetExample.decrypt(encryptedParquet.toString(), decryptedParquet.toString());

        assertTrue(Files.size(decryptedParquet) > 0);
    }
}
