// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import lombok.Builder;
import lombok.Value;

/**
 * Information needed for processing a Parquet files.
 */
@Value
@Builder
public class ParquetConfig {
    /**
     * An instance of ParquetConfig using only default values.
     */
    public static final ParquetConfig DEFAULT = new ParquetConfig(false);

    /**
     * Treat Parquet Binary values without annotations as if they had the string annotation.
     */
    private final Boolean binaryAsString;

    /**
     * Checks if any of the Parquet settings are configured. Used for validating that specified CLI options match file type.
     *
     * @return {@code true} If any configuration option is set.
     */
    public boolean isSet() {
        return binaryAsString != null;
    }
}
