// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

/**
 * To be implemented by any class that requires a validation step after being constructed/deserialized.
 */
public interface Validatable {
    /**
     * Checks type instance for validity. Used to ensure consistent construction of data types between GSON and Java generated instances.
     */
    void validate();
}
