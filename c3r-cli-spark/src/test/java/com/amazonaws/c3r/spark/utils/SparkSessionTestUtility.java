// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public abstract class SparkSessionTestUtility {

    /**
     * Initializes a SparkSession object with the passed Spark Drive URL.
     *
     * @return A SparkSession connected to the Spark Driver
     */
    public static SparkSession initSparkSession() {
        // CHECKSTYLE:OFF
        final SparkConf conf = new SparkConf()
                .setAppName("C3R")
                .setMaster("local[*]");
        // CHECKSTYLE:ON

        return SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
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