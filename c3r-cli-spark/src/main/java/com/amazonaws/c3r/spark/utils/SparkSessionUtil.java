// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.spark.sql.SparkSession;

/**
 * Utility class for Spark Session functionality.
 */
public abstract class SparkSessionUtil {

    /**
     * Initializes a SparkSession object with the passed Spark Drive URL.
     *
     * @return A SparkSession connected to the Spark Driver
     * @throws C3rRuntimeException if the Spark Driver cannot be connected to
     */
    public static SparkSession initSparkSession() {
        try {
            return SparkSession
                    .builder()
                    .appName("C3R")
                    .getOrCreate();
        } catch (Exception e) {
            throw new C3rRuntimeException("Could not connect to Spark server.", e);
        }
    }

    /**
     * Shut down the Spark session.
     *
     * @param spark the SparkSession to close
     */
    public static void closeSparkSession(final SparkSession spark) {
        spark.stop();
    }
}
