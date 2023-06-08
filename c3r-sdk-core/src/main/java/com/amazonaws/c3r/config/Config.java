// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import lombok.Getter;
import lombok.NonNull;

import javax.crypto.SecretKey;
import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Basic information needed whether encrypting or decrypting data.
 */
@Getter
public abstract class Config {
    /**
     * Location of input data.
     */
    private final String sourceFile;

    /**
     * Data type.
     */
    private final FileFormat fileFormat;

    /**
     * Where output should be saved.
     */
    private final String targetFile;

    /**
     * Whether to overwrite the target file.
     */
    private final boolean overwrite;

    /**
     * What value should be interpreted as {@code null} for CSV files.
     */
    private final String csvInputNullValue;

    /**
     * What value should be saved in output to represent {@code null} values for CSV.
     */
    private final String csvOutputNullValue;

    /**
     * Clean room key used to generate sub-keys for HMAC and encryption.
     */
    private final SecretKey secretKey;

    /**
     * Salt that can be publicly known but adds to randomness of cryptographic operations.
     */
    private final String salt;

    /**
     * Basic configuration information needed for encrypting or decrypting data.
     *
     * @param secretKey          Clean room key used to generate sub-keys for HMAC and encryption
     * @param sourceFile         Location of input data
     * @param fileFormat         Format of input data
     * @param targetFile         Where output should be saved
     * @param overwrite          Whether to overwrite the target file if it exists already
     * @param csvInputNullValue  What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue What value should be saved in output to represent {@code null} values for CSV
     * @param salt               Salt that can be publicly known but adds to randomness of cryptographic operations
     */
    protected Config(@NonNull final SecretKey secretKey, @NonNull final String sourceFile, final FileFormat fileFormat,
                     final String targetFile, final boolean overwrite, final String csvInputNullValue, final String csvOutputNullValue,
                     @NonNull final String salt) {
        this.secretKey = secretKey;
        this.sourceFile = sourceFile;
        this.fileFormat = fileFormat == null ? FileFormat.fromFileName(sourceFile) : fileFormat;
        this.targetFile = targetFile == null ? getDefaultTargetFile(sourceFile) : targetFile;
        this.overwrite = overwrite;
        this.csvInputNullValue = csvInputNullValue;
        this.csvOutputNullValue = csvOutputNullValue;
        this.salt = salt;

        validate();
    }

    /**
     * Get a default target file name based on a source file name, maintaining the file extension if one exists.
     *
     * @param sourceFile Name of source file
     * @return Default target name
     */
    static String getDefaultTargetFile(@NonNull final String sourceFile) {
        final File file = new File(sourceFile);
        final String sourceFileNameNoPath = file.getName();
        final int extensionIndex = sourceFileNameNoPath.lastIndexOf(".");
        if (extensionIndex < 0) {
            return sourceFileNameNoPath + ".out";
        } else {
            return sourceFileNameNoPath.substring(0, extensionIndex) + ".out" + sourceFileNameNoPath.substring(extensionIndex);
        }
    }

    /**
     * Verifies that settings are consistent.
     * - Make sure the shared secret is valid
     *
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    private void validate() {
        // Validates that a key can be derived early instead of waiting for Transformer initialization later.
        new DerivedRootEncryptionKey(secretKey, salt.getBytes(StandardCharsets.UTF_8));
    }
}
