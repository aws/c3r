// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for conveniently generating various command line argument
 * combinations for the `encrypt` command.
 */
@Setter
public final class EncryptCliConfigTestUtility {
    /**
     * Schema file location.
     */
    private String schema;

    /**
     * Collaboration ID to use for computing shared secret keys.
     */
    private String collaborationId;

    /**
     * Input file location.
     */
    @Getter
    private String input;

    /**
     * Value used in the input file to represent {@code null} in the CSV data.
     */
    private String csvInputNullValue;

    /**
     * Value to use in the output file to represent {@code null} in the CSV data.
     */
    private String csvOutputNullValue;

    /**
     * Location to write the output file.
     */
    @Getter
    private String output;

    /**
     * Whether the output file should be overwritten if it already exists.
     */
    private boolean overwrite;

    /**
     * Whether encryption will actually be run or only the configuration will be validated.
     */
    private boolean dryRun;

    /**
     * Whether plaintext values are allowed.
     */
    private boolean allowCleartext;

    /**
     * Whether duplicate values are allowed in fingerprint columns.
     */
    private boolean allowDuplicates;

    /**
     * Whether columns with different names should be allowed in a join statement.
     */
    private boolean allowJoinsOnColumnsWithDifferentNames;

    /**
     * Whether {@code null} values should be preserved during encryption.
     */
    private boolean preserveNulls;

    /**
     * Whether a stacktrace should be printed.
     */
    private boolean enableStackTraces;

    /**
     * Input file data type.
     */
    private FileFormat fileFormat;

    /**
     * AWS CLI profile.
     */
    private String profile;

    /**
     * AWS region.
     */
    private String region;

    /**
     * Hidden default constructor so static instance creators are used.
     */
    private EncryptCliConfigTestUtility() {
    }

    /**
     * Default test values for encryption args to use with tests.
     *
     * @return Default test values
     */
    public static EncryptCliConfigTestUtility defaultTestArgs() {
        final var args = new EncryptCliConfigTestUtility();
        args.enableStackTraces = true;
        args.allowCleartext = true;
        args.overwrite = true;
        args.schema = "mySchema";
        args.collaborationId = GeneralTestUtility.EXAMPLE_SALT.toString();
        args.input = "mySourceFile";
        return args;
    }

    /**
     * Creates a test configuration for a dry run. Skips all data processing and validates settings.
     *
     * @param file   Input file to use for the dry run
     * @param schema Schema file to use for the dry run
     * @return Default dry run configuration with specified files
     */
    public static EncryptCliConfigTestUtility defaultDryRunTestArgs(final String file, final String schema) {
        final var args = new EncryptCliConfigTestUtility();
        args.schema = (schema == null) ? "mySchema" : schema;
        args.collaborationId = GeneralTestUtility.EXAMPLE_SALT.toString();
        args.input = (file == null) ? "mySourceFile" : file;
        args.overwrite = true;
        args.dryRun = true;
        args.allowCleartext = true;
        args.enableStackTraces = true;
        return args;
    }

    /**
     * Empty CLI configuration.
     *
     * @return Configuration instance with no set values
     */
    public static EncryptCliConfigTestUtility blankTestArgs() {
        return new EncryptCliConfigTestUtility();
    }

    /**
     * Create an instance of {@code ClientSettings} using the specified values.
     *
     * @return {@link ClientSettings} using values stored in this instance
     */
    public ClientSettings getClientSettings() {
        return ClientSettings.builder()
                .allowCleartext(allowCleartext)
                .allowDuplicates(allowDuplicates)
                .allowJoinsOnColumnsWithDifferentNames(allowJoinsOnColumnsWithDifferentNames)
                .preserveNulls(preserveNulls).build();
    }

    /**
     * Converts the specified command line parameters to a list.
     *
     * @return List of command line parameters
     * @see EncryptCliConfigTestUtility#getCliArgsWithoutMode
     */
    public List<String> getCliArgs() {
        final List<String> args = new ArrayList<>();
        args.add("encrypt");
        if (input != null) {
            args.add(input);
        }
        if (schema != null) {
            args.add("--schema=" + schema);
        }
        if (collaborationId != null) {
            args.add("--id=" + collaborationId);
        }
        if (csvInputNullValue != null) {
            args.add("--csvInputNULLValue=" + csvInputNullValue);
        }
        if (csvOutputNullValue != null) {
            args.add("--csvOutputNULLValue=" + csvOutputNullValue);
        }
        if (output != null) {
            args.add("--output=" + output);
        }
        if (overwrite) {
            args.add("--overwrite");
        }
        if (dryRun) {
            args.add("--dryRun");
        }
        if (enableStackTraces) {
            args.add("--enableStackTraces");
        }
        if (fileFormat != null) {
            args.add("--fileFormat=" + fileFormat);
        }
        if (profile != null) {
            args.add("--profile=" + profile);
        }
        if (region != null) {
            args.add("--region=" + region);
        }
        return args;
    }

    /**
     * Converts the specified command line parameters to a list without including the CLI mode parameter.
     *
     * @return List of command line parameters.
     * @see EncryptCliConfigTestUtility#getCliArgs
     */
    public List<String> getCliArgsWithoutMode() {
        final List<String> args = getCliArgs();
        args.remove(0);
        return args;
    }

    /**
     * Converts the specified command line parameters to an array without including the CLI mode parameter.
     *
     * @return Array of command line parameters
     */
    public String[] toArrayWithoutMode() {
        return getCliArgsWithoutMode().toArray(String[]::new);
    }
}
