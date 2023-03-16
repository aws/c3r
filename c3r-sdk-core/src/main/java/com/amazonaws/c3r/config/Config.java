// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.File;

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
     * What value should be interpreted as {@code null} for CSV files.
     */
    private final String csvInputNullValue;

    /**
     * What value should be saved in output to represent {@code null} values for CSV.
     */
    private final String csvOutputNullValue;

    /**
     * Basic configuration information needed for encrypting or decrypting data.
     *
     * @param sourceFile         Location of input data
     * @param fileFormat         Format of input data
     * @param targetFile         Where output should be saved
     * @param overwrite          Whether to overwrite the target file if it exists already
     * @param csvInputNullValue  What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue What value should be saved in output to represent {@code null} values for CSV
     */
    protected Config(@NonNull final String sourceFile, final FileFormat fileFormat, final String targetFile, final boolean overwrite,
                     final String csvInputNullValue, final String csvOutputNullValue) {
        this.sourceFile = sourceFile;
        this.fileFormat = fileFormat == null ? FileFormat.fromFileName(sourceFile) : fileFormat;
        this.targetFile = targetFile == null ? getDefaultTargetFile(sourceFile) : targetFile;
        this.csvInputNullValue = csvInputNullValue;
        this.csvOutputNullValue = csvOutputNullValue;

        validate(overwrite);

        FileUtil.initFileIfNotExists(this.targetFile);
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
     * Make sure files can be accessed and the data format is known.
     *
     * @param overwrite Whether to overwrite the output file if it already exists
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    private void validate(final boolean overwrite) {
        FileUtil.verifyReadableFile(sourceFile);
        FileUtil.verifyWritableFile(targetFile, overwrite);

        if (fileFormat == null) {
            throw new C3rIllegalArgumentException("Unknown file extension: please specify the file format for file " + sourceFile + ".");
        }

        if (fileFormat != FileFormat.CSV) {
            if (csvInputNullValue != null || csvOutputNullValue != null) {
                throw new C3rIllegalArgumentException("CSV options specified for " + fileFormat + " file.");
            }
        }
    }
}
