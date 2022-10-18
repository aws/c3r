// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

/**
 * Differentiators for padding types.
 */
public enum PadType {
    /**
     * Values are not padded.
     */
    NONE,

    /**
     * Values are padded to a user-specified {@code PAD_LENGTH}.
     */
    FIXED,

    /**
     * Values are padded to {@code MAX_SIZE + PAD_LENGTH} where {@code MAX_SIZE} is the size of the
     * longest value in the column and {@code PAD_LENGTH} is user-specified.
     */
    MAX
}