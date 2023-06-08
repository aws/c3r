// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import lombok.Builder;
import lombok.Getter;

import javax.crypto.spec.SecretKeySpec;

/**
 * Basic Decryption settings.
 */
@Builder
@Getter
public class DecryptSdkConfigTestUtility {
    /**
     * Key to use for decryption.
     */
    @Builder.Default
    private SecretKeySpec key = null;

    /**
     * Salt for key generation.
     */
    @Builder.Default
    private String salt = null;

    /**
     * Input file.
     */
    @Builder.Default
    private String input = null;

    /**
     * Column header names.
     */
    @Builder.Default
    private String[] columnHeaders = null;
}
