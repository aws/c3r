// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples.spark;

import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.examples.utils.FileTestUtility;
import com.amazonaws.c3r.json.GsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkExampleTest {

    @Test
    public void roundTripTest() throws IOException {
        final Path source = Path.of("../samples/csv/data_sample_with_quotes.csv");
        final Path encryptTarget = FileTestUtility.createTempDir();
        final Path schemaFile = Path.of("../samples/schema/config_sample.json");
        final TableSchema schema = GsonUtil.fromJson(Files.readString(schemaFile), TableSchema.class);
        SparkExample.encrypt(source.toString(), encryptTarget.toString(), schema);
        final List<File> encryptedCsvs = Arrays.stream(Objects.requireNonNull(encryptTarget.toFile().listFiles()))
                .filter(file -> file.getAbsolutePath().endsWith(".csv"))
                .collect(Collectors.toList());
        for (File encryptedCsv : encryptedCsvs) {
            assertNotNull(encryptedCsv);
            assertTrue(encryptedCsv.exists());
            assertTrue(Files.size(encryptedCsv.toPath()) > 0);
        }
        final Path decryptTarget = FileTestUtility.createTempDir();
        for (File encryptedCsv : encryptedCsvs) {
            SparkExample.decrypt(encryptedCsv.getAbsolutePath(), decryptTarget.toString());
            assertNotNull(encryptedCsv);
            assertTrue(encryptedCsv.exists());
            assertTrue(Files.size(encryptedCsv.toPath()) > 0);
        }
        final List<File> decryptedCsvs = Arrays.stream(Objects.requireNonNull(decryptTarget.toFile().listFiles()))
                .filter(file -> file.getAbsolutePath().endsWith(".csv"))
                .collect(Collectors.toList());
        for (File decryptedCsv : decryptedCsvs) {
            assertNotNull(decryptedCsv);
            assertTrue(decryptedCsv.exists());
            assertTrue(Files.size(decryptedCsv.toPath()) > 0);
        }
    }
}
