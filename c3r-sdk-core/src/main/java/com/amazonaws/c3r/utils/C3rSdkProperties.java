// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import software.amazon.awssdk.core.ApiName;

/**
 * Utility class for storing C3R-wide constants.
 */
public final class C3rSdkProperties {

    /**
     * C3R version.
     */
    public static final String VERSION = "1.2.3";

    /**
     * C3R SDK user agent.
     */
    public static final ApiName API_NAME = ApiName.builder().name("c3r-sdk").version(VERSION).build();

    /**
     * Hidden utility class constructor.
     */
    private C3rSdkProperties() {
    }
}
