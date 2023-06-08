// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import com.amazonaws.c3r.config.ColumnType;
import lombok.Builder;

/**
 * Used to store performance testing metrics.
 */
@Builder
public class TimingResultTestUtility {
    /**
     * Header names for timing results.
     */
    public static final String[] HEADERS = {
            "Columns",
            "Rows",
            "Marshal Time (s)",
            "Unmarshal Time (s)",
            "Input Size (MB)",
            "Marshalled Size (MB)",
            "Unmarshalled Size (MB)",
            "Cleartext Columns",
            "Sealed Columns",
            "Fingerprint Columns",
            "Chars/Entry"
    };

    /**
     * How many column types we are supporting.
     */
    private static final int NUM_COL_TYPES = ColumnType.values().length;

    /**
     * Conversion factor for bytes to megabytes.
     */
    private static final double MB = Math.pow(2, 20);

    /**
     * How many characters per entry in the input file.
     */
    private Integer charsPerEntry;

    /**
     * Number of columns in the files.
     */
    private Integer columnCount;

    /**
     * Number of rows in the files.
     */
    private Long rowCount;

    /**
     * Size of original input file.
     */
    private Long inputSizeBytes;

    /**
     * Time spent marshalling data.
     */
    private Long marshalTimeSec;

    /**
     * Size of marshalled file.
     */
    private Long marshalledSizeBytes;

    /**
     * Time spent unmarshalling data.
     */
    private Long unmarshalTimeSec;

    /**
     * Size of the unmarshalled file.
     */
    private Long unmarshalledSizeBytes;

}