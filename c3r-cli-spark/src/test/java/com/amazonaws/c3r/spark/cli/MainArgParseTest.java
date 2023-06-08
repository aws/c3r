// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class for testing CLI argument parsing from the top-level which intentionally
 * does not execute any C3R business logic. I.e., only testing CLI parsing
 * configurations are correct with respect to which arguments are required,
 * which are exclusive, how certain common behaviors are triggered, etc.
 */
public class MainArgParseTest {
    @Test
    public void noArgsTest() {
        final CommandLine.ParseResult result = Main.getApp().parseArgs();
        assertFalse(result.isVersionHelpRequested());
        assertFalse(result.isUsageHelpRequested());
        assertEquals(0, result.subcommands().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"-V", "--version"})
    public void mainVersionTest(final String versionFlag) {
        final CommandLine.ParseResult result = Main.getApp().parseArgs(versionFlag);
        assertTrue(result.isVersionHelpRequested());
        assertFalse(result.isUsageHelpRequested());
        assertEquals(0, result.subcommands().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"-h", "--help"})
    public void mainHelpTest(final String helpFlag) {
        final CommandLine.ParseResult result = Main.getApp().parseArgs(helpFlag);
        assertFalse(result.isVersionHelpRequested());
        assertTrue(result.isUsageHelpRequested());
        assertEquals(0, result.subcommands().size());
    }

    /**
     * Check help parses as expected for a certain mode.
     *
     * @param mode CLI mode
     * @param help Help flag
     */
    private void checkModeHelpFlag(final String mode, final String help) {
        final CommandLine.ParseResult mainResult = Main.getApp().parseArgs(mode, help);
        assertEquals(1, mainResult.subcommands().size());
        final CommandLine.ParseResult modeResult = mainResult.subcommand();
        assertEquals(mode, modeResult.commandSpec().name());
        assertEquals(1, modeResult.expandedArgs().size());
        assertEquals(help, modeResult.expandedArgs().get(0));
        assertFalse(modeResult.isVersionHelpRequested());
        assertTrue(modeResult.isUsageHelpRequested());
    }

    @ParameterizedTest
    @ValueSource(strings = {"encrypt", "decrypt", "schema"})
    public void modeHelpFlagTest(final String mode) {
        checkModeHelpFlag(mode, "-h");
        checkModeHelpFlag(mode, "--help");
    }

    /**
     * Check version parses as expected for a certain mode.
     *
     * @param mode    CLI mode
     * @param version Version flag
     */
    private void checkModeVersionFlag(final String mode, final String version) {
        final CommandLine.ParseResult mainResult = Main.getApp().parseArgs(mode, version);
        assertEquals(1, mainResult.subcommands().size());
        final CommandLine.ParseResult modeResult = mainResult.subcommand();
        assertEquals(mode, modeResult.commandSpec().name());
        assertEquals(1, modeResult.expandedArgs().size());
        assertEquals(version, modeResult.expandedArgs().get(0));
        assertTrue(modeResult.isVersionHelpRequested());
        assertFalse(modeResult.isUsageHelpRequested());
    }

    @ParameterizedTest
    @ValueSource(strings = {"encrypt", "decrypt", "schema"})
    public void modeVersionFlagTest(final String mode) {
        checkModeVersionFlag(mode, "-V");
        checkModeVersionFlag(mode, "--version");
    }

    @ParameterizedTest
    @ValueSource(strings = {"encrypt", "decrypt", "schema"})
    public void subcommandsWithNoArgsTest(final String mode) {
        // NOTE: This assumes the above listed modes have _some_ required arguments
        assertThrows(CommandLine.MissingParameterException.class, () -> Main.getApp().parseArgs(mode));
    }

    @Test
    public void invalidSubcommandTest() {
        // NOTE: This assumes the above listed modes have _some_ required arguments
        assertThrows(CommandLine.UnmatchedArgumentException.class, () -> Main.getApp().parseArgs("not-a-real-mode"));
    }

    /**
     * Asserts that no errors occur when using the given minimal args,
     * and then asserts that removing any of the arguments after the
     * first (i.e., the mode name itself) raises an error and a missing parameter).
     *
     * @param minimalArgs Minimal argument list - first element is mode name, remaining are arguments
     *                    for that mode.
     */
    public void checkMinimalRequiredModeArgs(final String[] minimalArgs) {
        // NOTE: This assumes the above listed modes have _some_ required arguments
        assertDoesNotThrow(() -> Main.getApp().parseArgs(minimalArgs));

        // check that for this mode (element 0), removing any argument causes a CLI parse error
        for (int pos = 1; pos < minimalArgs.length; pos++) {
            final List<String> invalidParameters = Arrays.stream(minimalArgs).collect(Collectors.toList());
            invalidParameters.remove(pos);
            assertThrows(CommandLine.MissingParameterException.class, () ->
                    Main.getApp().parseArgs(invalidParameters.toArray(String[]::new)));
        }
    }

    @Test
    public void encryptWithRequiredArgs() {
        final String[] parameters = {"encrypt", "input", "--id=00000000-1111-2222-3333-444444444444", "--schema=schema"};
        checkMinimalRequiredModeArgs(parameters);
    }

    @Test
    public void decryptWithRequiredArgs() {
        final String[] parameters = {"decrypt", "input", "--id=00000000-1111-2222-3333-444444444444"};
        checkMinimalRequiredModeArgs(parameters);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-t", "--template", "-i", "--interactive"})
    public void schemaWithRequiredArgs(final String modeFlag) {
        final String[] parameters = {"schema", "input", modeFlag};
        checkMinimalRequiredModeArgs(parameters);
    }

    @Test
    public void schemaGenModesExclusiveArgs() {
        final String[] parameters = {"schema", "input", "-i", "-t"};
        // parsing with both -i and -t errors due to those being mutually exclusive
        assertThrows(CommandLine.MutuallyExclusiveArgsException.class, () -> Main.getApp().parseArgs(parameters));
        // and simply dropping one fixes things
        assertDoesNotThrow(() -> Main.getApp().parseArgs(Arrays.copyOfRange(parameters, 0, parameters.length - 1)));
    }

}
