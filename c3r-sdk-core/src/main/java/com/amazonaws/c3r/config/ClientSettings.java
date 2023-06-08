// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

/**
 * Contains clean room wide settings.
 */
@Value
@Builder
public class ClientSettings implements Serializable {
    /**
     * Whether cleartext columns are allowed.
     */
    private boolean allowCleartext;

    /**
     * Whether duplicate values are allowed in a fingerprint column.
     *
     * @see ColumnType#FINGERPRINT
     */
    private boolean allowDuplicates;

    /**
     * Whether fingerprint column names need to match on queries.
     *
     * @see ColumnType#FINGERPRINT
     */
    private boolean allowJoinsOnColumnsWithDifferentNames;

    /**
     * Whether {@code null} values should be encrypted or left as {@code null}.
     */
    private boolean preserveNulls;

    /**
     * Most permissive settings.
     *
     * @return ClientSettings with all flags set to `true`
     */
    public static ClientSettings lowAssuranceMode() {
        return ClientSettings.builder()
                .allowCleartext(true)
                .allowDuplicates(true)
                .allowJoinsOnColumnsWithDifferentNames(true)
                .preserveNulls(true)
                .build();
    }

    /**
     * Least permissive settings.
     *
     * @return ClientSettings with all flags set to `false`
     */
    public static ClientSettings highAssuranceMode() {
        return ClientSettings.builder()
                .allowCleartext(false)
                .allowDuplicates(false)
                .allowJoinsOnColumnsWithDifferentNames(false)
                .preserveNulls(false)
                .build();
    }
}