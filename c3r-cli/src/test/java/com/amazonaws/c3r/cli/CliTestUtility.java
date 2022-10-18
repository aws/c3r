// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import picocli.CommandLine;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utilities to interface with the CLI interface as if you were calling from the command line.
 */
public final class CliTestUtility {
    /**
     * Hidden utility class constructor.
     */
    private CliTestUtility() {
    }

    /**
     * Function to test cli options without loading the entire system. First argument must be one of the sub-modes:
     * encrypt, decrypt, schema. From there, the rest of the parameters should match what is required of that particular sub-mode. If extra
     * parameters are present, an error will be thrown or if required parameters are missing and error will be thrown.
     *
     * @param args Set of strings corresponding to a run of the software
     * @return A data structure that stores all stages of parsing from initial reading of parameters until final matches are made
     */
    public static CommandLine.ParseResult verifyCliOptions(final String[] args) {
        return Main.getApp().parseArgs(args);
    }

    /**
     * Runs the cli with a mock to replace an actual connection to AWS Clean Rooms.
     *
     * @param args Command line parameters for encrypt mode
     * @return {@value Main#SUCCESS} if no errors are encountered or {@value Main#FAILURE}
     */
    public static int runWithoutCleanRooms(final EncryptCliConfigTestUtility args) {
        final CleanRoomsDao cleanRoomsDao;
        cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(args.getClientSettings());
        return EncryptMode.getApp(cleanRoomsDao).execute(args.toArrayWithoutMode());
    }
}
