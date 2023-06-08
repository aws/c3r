// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A test utility for creating temporary Path resources for tests that will clean themselves up after execution.
 */
public abstract class FileTestUtility {

    /**
     * Creates a temporary directory with the prefix "temp" marked with deleteOnExit.
     *
     * @return A temporary Path
     * @throws IOException If the temporary Path cannot be created
     */
    public static Path createTempDir() throws IOException {
        final Path tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        return tempDir;
    }

    /**
     * Creates a temporary file with the prefix "testFile" and suffix ".tmp" marked with deleteOnExit.
     *
     * @return A temporary Path
     * @throws IOException If the temporary Path cannot be created
     */
    public static Path createTempFile() throws IOException {
        return createTempFile("testFile", ".tmp");
    }

    /**
     * Creates a temporary file with the prefix and suffix provided marked with deleteOnExit.
     *
     * @param prefix The prefix of the Path to create
     * @param suffix The suffix of the Path to create
     * @return A temporary Path
     * @throws IOException If the temporary Path cannot be created
     */
    public static Path createTempFile(final String prefix, final String suffix) throws IOException {
        final Path tempDir = createTempDir();
        final Path tempFile = Files.createTempFile(tempDir, prefix, suffix);
        tempFile.toFile().deleteOnExit();
        return tempFile;
    }

    /**
     * Resolves a temporary file with the file name provided marked with deleteOnExit.
     *
     * @param fileName The name of the Path to resolve
     * @return A temporary Path
     * @throws IOException If the temporary Path cannot be resolved
     */
    public static Path resolve(final String fileName) throws IOException {
        return resolve(fileName, createTempDir());
    }

    /**
     * Resolves a temporary file with the prefix and suffix provided marked with deleteOnExit.
     *
     * @param fileName The name of the Path to resolve
     * @param tempDir  The Path to use to resolve the temporary file
     * @return A temporary Path
     */
    private static Path resolve(final String fileName, final Path tempDir) {
        final Path tempFile = tempDir.resolve(fileName);
        tempFile.toFile().deleteOnExit();
        return tempFile;
    }

}
