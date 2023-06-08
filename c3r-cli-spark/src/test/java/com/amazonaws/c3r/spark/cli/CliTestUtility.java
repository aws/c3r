// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.spark.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;

import static org.mockito.ArgumentMatchers.any;
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
     * Runs the cli with a mock to replace an actual connection to AWS Clean Rooms.
     *
     * @param args Command line parameters for encrypt mode
     * @return {@value Main#SUCCESS} if no errors are encountered or {@value Main#FAILURE}
     */
    public static int runWithoutCleanRooms(final EncryptCliConfigTestUtility args) {
        final CleanRoomsDao cleanRoomsDao;
        cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(args.getClientSettings());
        return EncryptMode.getApp(cleanRoomsDao, SparkSessionTestUtility.initSparkSession())
                .execute(args.toArrayWithoutMode());
    }
}
