// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for conveniently generating various command line argument
 * combinations for the `decrypt` command.
 */
@Setter
public final class DecryptCliConfigTestUtility {
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
     * Whether to fail if a fingerprint column is seen in the data file.
     */
    private boolean failOnFingerprintColumns;

    /**
     * Whether a stacktrace should be printed.
     */
    private boolean enableStackTraces;

    /**
     * Input file data type.
     */
    private FileFormat fileFormat;

    /**
     * Hidden default constructor so static instance creators are used.
     */
    private DecryptCliConfigTestUtility() {
    }

    /**
     * Default test values for encryption args to use with tests.
     *
     * @return Default test values
     */
    public static DecryptCliConfigTestUtility defaultTestArgs() {
        final var args = new DecryptCliConfigTestUtility();
        args.enableStackTraces = true;
        args.overwrite = true;
        args.collaborationId = GeneralTestUtility.EXAMPLE_SALT.toString();
        args.input = "mySourceFile";
        return args;
    }

    /**
     * Creates a test configuration for a dry run. Skips all data processing and validates settings.
     *
     * @param file Input file to use for the dry run
     * @return Default dry run configuration
     */
    public static DecryptCliConfigTestUtility defaultDryRunTestArgs(final String file) {
        final var args = new DecryptCliConfigTestUtility();
        args.collaborationId = GeneralTestUtility.EXAMPLE_SALT.toString();
        args.input = (file == null) ? "mySourceFile" : file;
        args.overwrite = true;
        args.dryRun = true;
        args.enableStackTraces = true;
        return args;
    }

    /**
     * Empty CLI configuration.
     *
     * @return Configuration instance with no set values
     */
    public static DecryptCliConfigTestUtility blankTestArgs() {
        return new DecryptCliConfigTestUtility();
    }

    /**
     * Converts the specified command line parameters to a list.
     *
     * @return List of command line parameters
     * @see DecryptCliConfigTestUtility#getCliArgsWithoutMode
     */
    public List<String> getCliArgs() {
        final List<String> args = new ArrayList<>();
        args.add("decrypt");
        if (input != null) {
            args.add(input);
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
        if (failOnFingerprintColumns) {
            args.add("--failOnFingerprintColumns");
        }
        if (enableStackTraces) {
            args.add("--enableStackTraces");
        }
        if (fileFormat != null) {
            args.add("--fileFormat=" + fileFormat);
        }
        return args;
    }

    /**
     * Converts the specified command line parameters to a list without including the CLI mode parameter.
     *
     * @return List of command line parameters.
     * @see DecryptCliConfigTestUtility#getCliArgs
     */
    public List<String> getCliArgsWithoutMode() {
        final List<String> args = getCliArgs();
        args.remove(0);
        return args;
    }

    /**
     * Converts the specified command line parameters to an array.
     *
     * @return Array of command line parameters
     * @see DecryptCliConfigTestUtility#toArrayWithoutMode
     */
    public String[] toArray() {
        return getCliArgs().toArray(String[]::new);
    }

    /**
     * Converts the specified command line parameters to an array without including the CLI mode parameter.
     *
     * @return Array of command line parameters
     * @see DecryptCliConfigTestUtility#toArray
     */
    public String[] toArrayWithoutMode() {
        return getCliArgsWithoutMode().toArray(String[]::new);
    }
}
