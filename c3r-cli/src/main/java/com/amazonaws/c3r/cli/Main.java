// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

/**
 * Top level class for the CLI. Global options such as how to handle command line parsing are configured here and then
 * subcommand specific options are configured in each subcommand.
 */
@Slf4j
@CommandLine.Command(
        name = "c3r-cli",
        mixinStandardHelpOptions = true,
        version = CliDescriptions.VERSION,
        description = "Cryptographic computing tool for use with AWS Clean Rooms.",
        subcommands = {SchemaMode.class, EncryptMode.class, DecryptMode.class})
public final class Main {
    /**
     * Return value to indicate a child subcommand ran successfully.
     */
    public static final int SUCCESS = 0;

    /**
     * Return value to indicate a child subcommand did not finish successfully.
     * Further information about the failure will be in the logs/CLI.
     */
    public static final int FAILURE = 1;

    /**
     * Create instance of the command line interface for all child subcommands.
     */
    private Main() {
    }

    /**
     * Get a copy of the application without passing in arguments yet.
     * NOTE: The object keeps state between calls so if you include a boolean flag on one run and not on the next,
     * the flag will still evaluate to true
     *
     * @return CommandLine interface to utility that you can use to add additional logging or information to
     */
    static CommandLine getApp() {
        return generateCommandLine(new Main());
    }

    /**
     * Constructs a new CommandLine interpreter with the specified object with picocli annotations.
     *
     * @param command The object with appropriate picocli annotations.
     * @return The constructed command line interpreter.
     */
    static CommandLine generateCommandLine(final Object command) {
        return new CommandLine(command).setTrimQuotes(true).setCaseInsensitiveEnumValuesAllowed(true);
    }

    /**
     * Handle top level logging of errors during execution.
     *
     * @param e                 Error encountered
     * @param enableStackTraces Whether the full stacktrace should be printed
     */
    static void handleException(final Exception e, final boolean enableStackTraces) {
        if (enableStackTraces) {
            log.error("An error occurred: {}", e.getMessage(), e);
        } else if (e instanceof C3rRuntimeException) {
            log.error("An error occurred: {}", e.getMessage());
        } else {
            log.error("An unexpected error occurred: {}", e.getClass());
            log.error("Note: the --enableStackTraces flag can provide additional context for errors.");
        }
        log.warn("Output files may have been left on disk.");
    }

    /**
     * Execute the application with a particular set of arguments.
     *
     * @param args Set of strings containing the options to use on this execution pass
     */
    public static void main(final String[] args) {
        final int exitCode = getApp().execute(args);
        System.exit(exitCode);
    }
}
