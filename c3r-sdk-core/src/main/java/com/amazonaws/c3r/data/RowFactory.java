// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

/**
 * Used to create rows for a particular data format.
 *
 * @param <T> Data format
 */
public interface RowFactory<T extends Value> {
    /**
     * Create an empty row to be populated by the callee.
     *
     * @return An empty Row for storing data in
     */
    Row<T> newRow();
}
