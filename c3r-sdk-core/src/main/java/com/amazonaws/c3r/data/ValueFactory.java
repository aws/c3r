// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

/**
 * Used to create values or containers of values for a particular data format.
 *
 * @param <T> Data format
 */
public interface ValueFactory<T extends Value> {
    /**
     * Takes a byte array of an encoded value and recreates the original value.
     *
     * @param bytes Encoded value
     * @return Recreated value
     */
    T createValueFromEncodedBytes(byte[] bytes);

    /**
     * Takes a byte array of an encoded value and gets the bytes representing the internal value.
     *
     * @param bytes Encoded value
     * @return Bytes containing the value without metadata
     */
    byte[] getValueBytesFromEncodedBytes(byte[] bytes);

    /**
     * Create an empty row to be populated by the callee.
     *
     * @return An empty Row for storing data in
     */
    Row<T> newRow();
}
