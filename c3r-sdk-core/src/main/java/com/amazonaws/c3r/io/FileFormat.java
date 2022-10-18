// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides support for file format type information such as file extensions.
 */
public enum FileFormat {
    /**
     * CSV formatted file.
     */
    CSV(".csv"),
    /**
     * Parquet formatted file.
     */
    PARQUET(".parquet");

    /**
     * Lookup table to map string extension to enum representation of extension.
     */
    private static final Map<String, FileFormat> EXTENSIONS = Arrays.stream(FileFormat.values()).collect(
            Collectors.toMap(
                    fmt -> fmt.extension,
                    Function.identity()
            ));
    /**
     * String containing the file extension.
     */
    private final String extension;

    /**
     * Construct a FileFormat with the given file extension.
     *
     * @param extension Supported file type extension to use
     */
    FileFormat(final String extension) {
        this.extension = extension;
    }

    /**
     * Check and see if the input file name has an extension specifying the file type.
     *
     * @param fileName Input file name
     * @return Supported data type or {@code null}
     */
    public static FileFormat fromFileName(final String fileName) {
        final int extensionStart = fileName.lastIndexOf('.');
        if (extensionStart < 0) {
            return null;
        }
        return EXTENSIONS.get(fileName.substring(extensionStart).toLowerCase());

    }
}
