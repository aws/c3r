// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

/**
 * CliDescriptions contains the help mode description for all CLI parameters, so they are consistently described across classes.
 */
public final class CliDescriptions {
    /**
     * Description of AWS profile.
     */
    public static final String AWS_PROFILE_DESCRIPTION = "AWS CLI profile for credentials and config (uses AWS "
            + " SDK default if omitted)";

    /**
     * Description of AWS region.
     */
    public static final String AWS_REGION_DESCRIPTION = "AWS region for API requests (uses AWS SDK default if omitted)";

    /**
     * Description of how to allow for custom CSV values in the input file.
     */
    public static final String ENCRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION = "Value representing how NULL is encoded in the input CSV data " +
            "(unquoted blank values and empty quotes are all interpreted as NULL (e.g., `,,`, `, ,` and `,\"\",`) by default)";

    /**
     * Description of how to allow for custom CSV NULL values in the encrypted output file.
     */
    public static final String ENCRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION = "The encoding of cleartext NULL values in the output file " +
            "(encrypted NULLs are encoded unambiguously, cleartext values default to the empty value `,,`)";

    /**
     * Description of how to allow for custom CSV NULL value interpretation in the encrypted input file.
     */
    public static final String DECRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION = "Value representing how the cleartext NULL value is encoded in" +
            " the input CSV data (defaults to `,,` for cleartext fields is interpreted as NULL as encrypted NULLs are encoded " +
            "unambiguously)";

    /**
     * Description of how to allow for custom CSV Null values in the decrypted output file.
     */
    public static final String DECRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION = "How a cleartext NULL value is encoded in the output file " +
            "(defaults to the empty value `,,`)";

    /**
     * Explanation of dry run mode.
     */
    public static final String DRY_RUN_DESCRIPTION = "Check settings and files to verify configuration is valid but skip processing " +
            "the input file";

    /**
     * Explanation and warnings about enabling stack traces.
     */
    public static final String ENABLE_STACKTRACE_DESCRIPTION = "Enable stack traces (WARNING: stack traces may contain sensitive info)";

    /**
     * List of acceptable file formats that can be specified.
     */
    public static final String FILE_FORMAT_DESCRIPTION = "File format of <input>: ${COMPLETION-CANDIDATES}";

    /**
     * Explanation of allowing Fingerprint columns to pass through for debugging.
     */
    public static final String FAIL_ON_FINGERPRINT_COLUMNS_DESCRIPTION = "Fail when encountering a fingerprint column during decryption " +
            "(disabled by default)";

    /**
     * Setting that allows for overwriting a file.
     */
    public static final String OVERWRITE_DESCRIPTION = "If output file exists, overwrite the file";

    /**
     * Setting for collaboration ID.
     */
    public static final String ID_DESCRIPTION = "Unique identifier for the collaboration. " +
            "Follows the pattern [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    /**
     * Description of data input source setting (note: CSV not specified in anticipation of future formats).
     */
    public static final String INPUT_DESCRIPTION_CRYPTO = "Data to be processed";

    /**
     * Description of schema data source.
     */
    public static final String INPUT_DESCRIPTION_SCHEMA = "Tabular file used for schema generation";

    /**
     * Description of output file naming when using CSV files.
     */
    public static final String OUTPUT_DESCRIPTION_CRYPTO = "Output directory (defaults to `output`)";

    /**
     * Description of output file naming for schema creation.
     */
    public static final String OUTPUT_DESCRIPTION_SCHEMA = "Output file name (defaults to `<input>`.json)";

    /**
     * Schema file location.
     */
    public static final String SCHEMA_DESCRIPTION = "JSON file specifying table transformations";

    /**
     * Hidden constructor since this is a utility class.
     */
    private CliDescriptions() {
    }
}