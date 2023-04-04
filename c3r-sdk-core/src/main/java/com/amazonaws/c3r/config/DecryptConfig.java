// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.io.FileFormat;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import javax.crypto.SecretKey;

/**
 * Information needed when decrypting a data file.
 */
@Getter
public final class DecryptConfig extends Config {

    /**
     * Whether to throw an error if a Fingerprint column is seen in the data.
     */
    private final boolean failOnFingerprintColumns;

    /**
     * Set up configuration that will be used for decrypting data.
     *
     * @param secretKey                Clean room key used to generate sub-keys for HMAC and encryption
     * @param sourceFile               Location of input data
     * @param fileFormat               Format of input data
     * @param targetFile               Where output should be saved
     * @param overwrite                Whether to overwrite the target file if it exists already
     * @param csvInputNullValue        What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue       What value should be saved in output to represent {@code null} values for CSV
     * @param salt                     Salt that can be publicly known but adds to randomness of cryptographic operations
     * @param failOnFingerprintColumns Whether to throw an error if a Fingerprint column is seen in the data
     */
    @Builder
    private DecryptConfig(@NonNull final SecretKey secretKey,
                          @NonNull final String sourceFile,
                          final FileFormat fileFormat,
                          final String targetFile,
                          final boolean overwrite,
                          final String csvInputNullValue,
                          final String csvOutputNullValue,
                          @NonNull final String salt,
                          final boolean failOnFingerprintColumns) {
        super(secretKey, sourceFile, fileFormat, targetFile, overwrite, csvInputNullValue, csvOutputNullValue, salt,
                Transformer.initTransformers(secretKey, salt, null, failOnFingerprintColumns));
        this.failOnFingerprintColumns = failOnFingerprintColumns;
    }

}
