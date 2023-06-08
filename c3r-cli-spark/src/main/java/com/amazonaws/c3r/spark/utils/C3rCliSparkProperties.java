// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import com.amazonaws.c3r.utils.C3rSdkProperties;
import software.amazon.awssdk.core.ApiName;

/**
 * C3R CLI for Apache Spark properties.
 */
public final class C3rCliSparkProperties {

    /**
     * Application name of C3R CLI client for Apache Spark.
     */
    public static final String APP_NAME = "c3r-cli-spark";

    /**
     * User agent for the C3R CLI.
     */
    public static final ApiName API_NAME = ApiName.builder()
            .name(APP_NAME)
            .version(C3rSdkProperties.VERSION)
            .build();

    /**
     * Hidden utility class constructor.
     */
    private C3rCliSparkProperties() {
    }
}
