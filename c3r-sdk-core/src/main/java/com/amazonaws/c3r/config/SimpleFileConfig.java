// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.NonNull;

import javax.crypto.SecretKey;

/**
 * Basic information needed whether encrypting or decrypting data for basic file types.
 */
public abstract class SimpleFileConfig extends Config {

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
    protected SimpleFileConfig(@NonNull final SecretKey secretKey, @NonNull final String sourceFile, final FileFormat fileFormat,
                               final String targetFile, final boolean overwrite, final String csvInputNullValue,
                               final String csvOutputNullValue, @NonNull final String salt) {
        super(secretKey, sourceFile, fileFormat, targetFile, overwrite, csvInputNullValue, csvOutputNullValue, salt);

        validate();
    }

    /**
     * Verifies that settings are consistent.
     * - Make sure the program can read from the source file
     * - Make sure the program can write to the target file
     * - Make sure the data format is known
     *
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    private void validate() {
        FileUtil.verifyReadableFile(getSourceFile());
        FileUtil.verifyWritableFile(getTargetFile(), isOverwrite());

        if (getFileFormat() == null) {
            throw new C3rIllegalArgumentException("Unknown file extension: please specify the file format for file "
                    + getSourceFile() + ".");
        }

        if (getFileFormat() != FileFormat.CSV) {
            if (getCsvInputNullValue() != null || getCsvOutputNullValue() != null) {
                throw new C3rIllegalArgumentException("CSV options specified for " + getFileFormat() + " file.");
            }
        }
    }
}
