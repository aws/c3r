// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.TableSchema;
import lombok.Builder;
import lombok.Getter;

import javax.crypto.spec.SecretKeySpec;
import java.util.List;

/**
 * Basic configuration settings for encryption.
 */
@Builder
@Getter
public class EncryptSdkConfigTestUtility {
    /**
     * Schema specification.
     */
    @Builder.Default
    private TableSchema schema = null;

    /**
     * Key to use for encryption.
     */
    @Builder.Default
    private SecretKeySpec key = null;

    /**
     * Salt to use for key generation.
     */
    @Builder.Default
    private String salt = null;

    /**
     * Security related parameters.
     */
    @Builder.Default
    private ClientSettings settings = ClientSettings.lowAssuranceMode();

    /**
     * Input file.
     */
    @Builder.Default
    private String input = null;

    /**
     * Column headers in the input file.
     */
    @Builder.Default
    private List<String> inputColumnHeaders = null;

    /**
     * Column headers to use in the output file.
     */
    @Builder.Default
    private List<String> outputColumnHeaders = null;
}
