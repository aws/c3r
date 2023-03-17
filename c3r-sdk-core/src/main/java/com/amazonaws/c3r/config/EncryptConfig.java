// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Information needed when encrypting a data file.
 */
@Getter
public final class EncryptConfig extends Config {
    /**
     * Directory to write temporary files in if two passes are needed to encrypt the data.
     */
    private final String tempDir;

    /**
     * Clean room cryptographic settings.
     */
    private final ClientSettings settings;

    /**
     * How the data in the input file maps to data in the output file.
     */
    private final TableSchema tableSchema;

    /**
     * Cryptographic transforms available.
     */
    private final Map<ColumnType, Transformer> transformers;

    /**
     * Set up configuration that will be used for encrypting data.
     *
     * @param secretKey Clean room key used to generate sub-keys for HMAC and encryption
     * @param sourceFile Location of input data
     * @param fileFormat Format of input data
     * @param targetFile Where output should be saved
     * @param tempDir Where to write temporary files if needed
     * @param overwrite Whether to overwrite the target file if it exists already
     * @param csvInputNullValue What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue What value should be saved in output to represent {@code null} values for CSV
     * @param salt Salt that can be publicly known but adds to randomness of cryptographic operations
     * @param settings Clean room cryptographic settings
     * @param tableSchema How data in the input file maps to data in the output file
     */
    @Builder
    private EncryptConfig(@NonNull final SecretKey secretKey,
                          @NonNull final String sourceFile,
                          final FileFormat fileFormat,
                          final String targetFile,
                          @NonNull final String tempDir,
                          final boolean overwrite,
                          final String csvInputNullValue,
                          final String csvOutputNullValue,
                          @NonNull final String salt,
                          @NonNull final ClientSettings settings,
                          @NonNull final TableSchema tableSchema) {
        super(sourceFile, fileFormat, targetFile, overwrite, csvInputNullValue, csvOutputNullValue);
        this.tempDir = tempDir;
        this.settings = settings;
        this.tableSchema = tableSchema;
        transformers = Transformer.initTransformers(secretKey, salt, settings, false);
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
        FileUtil.verifyWriteableDirectory(tempDir);

        if (!settings.isAllowCleartext()) {
            final Map<ColumnType, List<ColumnSchema>> typeMap = tableSchema.getColumns().stream()
                    .collect(Collectors.groupingBy(ColumnSchema::getType));
            if (typeMap.containsKey(ColumnType.CLEARTEXT)) {
                final String targetColumns = typeMap.get(ColumnType.CLEARTEXT).stream()
                        .map(column -> column.getTargetHeader().toString())
                        .collect(Collectors.joining("`, `"));
                throw new C3rIllegalArgumentException(
                        "Cleartext columns found in the schema, but allowCleartext is false. Target " +
                                "column names: [`" + targetColumns + "`]");
            }
        }
    }
}
