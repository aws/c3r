// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.config;

import com.amazonaws.c3r.config.Config;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.NonNull;

import javax.crypto.SecretKey;
import java.nio.file.Path;

/**
 * Basic information needed whether encrypting or decrypting data for Spark types.
 */
public class SparkConfig extends Config {

    /**
     * {@link org.apache.spark.sql.util.CaseInsensitiveStringMap} key for whether header normalization
     * is skipped.
     */
    public static final String PROPERTY_KEY_SKIP_HEADER_NORMALIZATION = "skipHeaderNormalization";

    /**
     * Basic configuration information needed for encrypting or decrypting data.
     *
     * @param secretKey          Clean room key used to generate sub-keys for HMAC and encryption
     * @param source             Location of input data
     * @param fileFormat         Format of input data
     * @param targetDir          Where output should be saved
     * @param overwrite          Whether to overwrite the target file if it exists already
     * @param csvInputNullValue  What value should be interpreted as {@code null} for CSV files
     * @param csvOutputNullValue What value should be saved in output to represent {@code null} values for CSV
     * @param salt               Salt that can be publicly known but adds to randomness of cryptographic operations
     */
    protected SparkConfig(@NonNull final SecretKey secretKey, @NonNull final String source, final FileFormat fileFormat,
                          final String targetDir, final boolean overwrite, final String csvInputNullValue,
                          final String csvOutputNullValue, @NonNull final String salt) {
        super(secretKey, source, fileFormat, (targetDir == null ? "output" : targetDir),
                overwrite, csvInputNullValue, csvOutputNullValue, salt);

        validate();
    }

    /**
     * Verifies
    /**
     * Verifies that settings are consistent.
     * - Make sure the program can write to the target directory
     * - Make sure the source can be read from
     * - If the input is a file, make sure the source file format is supported
     * - If the input is a directory, make sure a file format was specified
     * - If the source file format is not CSV, ensure no CSV configuration parameters are set
     *
     * @throws C3rIllegalArgumentException If any of the rules are violated
     */
    private void validate() {
        FileUtil.verifyWritableDirectory(getTargetFile(), isOverwrite());

        final Path source = Path.of(getSourceFile());
        if (source.toFile().isFile()) {
            FileUtil.verifyReadableFile(getSourceFile());
            if (getFileFormat() == null) {
                throw new C3rIllegalArgumentException("Unknown file extension: please specify the file format for file "
                        + getSourceFile() + ".");
            }
        } else {
            FileUtil.verifyReadableDirectory(getSourceFile());
            if (getFileFormat() == null) {
                throw new C3rIllegalArgumentException("An input file format must be selected if providing a source directory.");
            }
        }
        
        if (getFileFormat() != FileFormat.CSV) {
            if (getCsvInputNullValue() != null || getCsvOutputNullValue() != null) {
                throw new C3rIllegalArgumentException("CSV options specified for " + getFileFormat() + " file.");
            }
        }
    }
}
