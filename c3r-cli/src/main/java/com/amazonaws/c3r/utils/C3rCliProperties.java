// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.cli.CliDescriptions;
import software.amazon.awssdk.core.ApiName;

/**
 * C3R CLI properties.
 */
public final class C3rCliProperties {
    /**
     * User agent for the C3R CLI.
     */
    public static final ApiName API_NAME = ApiName.builder()
            .name(CliDescriptions.APP_NAME)
            .version(C3rSdkProperties.VERSION)
            .build();

    /**
     * Hidden utility class constructor.
     */
    private C3rCliProperties() {
    }
}
