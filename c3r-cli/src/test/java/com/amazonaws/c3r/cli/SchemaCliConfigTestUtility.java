// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.io.FileFormat;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for conveniently generating various command line argument
 * combinations for the `schema` command.
 */
@Builder
@Setter
public class SchemaCliConfigTestUtility {
    /**
     * Whether template or interactive mode should be used.
     */
    @Builder.Default
    private String subMode = "--template";

    /**
     * Data file for building the schema.
     */
    @Builder.Default
    private String input = "mySourceFile";

    /**
     * Output file location for the schema.
     */
    @Builder.Default
    @Getter
    private String output = null;

    /**
     * Whether the output file should be overwritten if it exists.
     */
    @Builder.Default
    private boolean overwrite = false;

    /**
     * How much detail is printed to the console and log files.
     */
    @Builder.Default
    private String verbosity = null;

    /**
     * Whether a stacktrace should be displayed.
     */
    @Builder.Default
    private boolean enableStackTraces = true;

    /**
     * Data type.
     */
    private FileFormat fileFormat;

    /**
     * If the data file has no headers.
     */
    @Builder.Default
    private boolean noHeaders = false;

    /**
     * Collaboration ID.
     */
    private String collaborationId;

    /**
     * Converts the specified command line parameters to a list.
     *
     * @return List of command line parameters
     * @see SchemaCliConfigTestUtility#toListWithoutMode
     */
    public List<String> toList() {
        final List<String> args = new ArrayList<>(List.of(
                "schema",
                input
        ));
        args.add(subMode);
        if (output != null) {
            args.add("--output=" + output);
        }
        if (overwrite) {
            args.add("--overwrite");
        }
        if (verbosity != null) {
            args.add("--verbosity=" + verbosity);
        }
        if (enableStackTraces) {
            args.add("--enableStackTraces");
        }
        if (fileFormat != null) {
            args.add("--fileFormat=" + fileFormat);
        }
        if (noHeaders) {
            args.add("--noHeaders");
        }
        if (collaborationId != null) {
            args.add("--id=" + collaborationId);
        }
        return args;
    }

    /**
     * Converts the specified command line parameters to a list without including the CLI mode parameter.
     *
     * @return List of command line parameters.
     * @see SchemaCliConfigTestUtility#toList
     */
    public List<String> toListWithoutMode() {
        final List<String> args = toList();
        args.remove(0);
        return args;
    }

    /**
     * Converts the specified command line parameters to an array.
     *
     * @return Array of command line parameters
     * @see SchemaCliConfigTestUtility#toArrayWithoutMode
     */
    public String[] toArray() {
        return toList().toArray(String[]::new);
    }

    /**
     * Converts the specified command line parameters to an array without including the CLI mode parameter.
     *
     * @return Array of command line parameters
     * @see SchemaCliConfigTestUtility#toArray
     */
    public String[] toArrayWithoutMode() {
        return toListWithoutMode().toArray(String[]::new);
    }
}
