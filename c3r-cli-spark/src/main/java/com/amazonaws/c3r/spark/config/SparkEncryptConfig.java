// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.config;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import javax.crypto.SecretKey;

/**
 * Information needed when encrypting a data file.
 */
@Getter
public final class SparkEncryptConfig extends SparkConfig {
    /**
     * Clean room cryptographic settings.
     */
    private final ClientSettings settings;

    /**
     * How the data in the input file maps to data in the output file.
     */
    private final TableSchema tableSchema;

    /**
     * Set up configuration that will be used for encrypting data.
     *
     * @param secretKey          Clean room key used to generate sub-keys for HMAC and encryption
     * @param source             Location of input data
     * @param fileFormat         Format of input data
     * @param targetDir          Where output should be saved
     * @param overwrite          Whether to overwrite the target file if it exists already
     * @param csvInputNullValue  What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue What value should be saved in output to represent {@code null} values for CSV
     * @param salt               Salt that can be publicly known but adds to randomness of cryptographic operations
     * @param settings           Clean room cryptographic settings
     * @param tableSchema        How data in the input file maps to data in the output file
     */
    @Builder
    private SparkEncryptConfig(@NonNull final SecretKey secretKey,
                               @NonNull final String source,
                               final FileFormat fileFormat,
                               final String targetDir,
                               final boolean overwrite,
                               final String csvInputNullValue,
                               final String csvOutputNullValue,
                               @NonNull final String salt,
                               @NonNull final ClientSettings settings,
                               @NonNull final TableSchema tableSchema) {
        super(secretKey, source, fileFormat, targetDir, overwrite, csvInputNullValue,
                csvOutputNullValue, salt);
        this.settings = settings;
        this.tableSchema = tableSchema;
        validate();
    }

    /**
     * Verifies that settings are consistent.
     * - Make sure the program can write to the temporary file directory
     * - If the clean room doesn't allow cleartext columns, verify none are in the schema
     *
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    private void validate() {
        TableSchema.validateSchemaAgainstClientSettings(tableSchema, settings);
    }
}
