// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.Value;
import lombok.NonNull;

import java.util.Collection;

/**
 * Resource for storing record entries to some store/backend.
 *
 * @param <T> Specific data type that will be written
 */
public interface RowWriter<T extends Value> {
    /**
     * Gets the headers for the output file.
     *
     * @return The ColumnHeaders of the output file
     */
    Collection<ColumnHeader> getHeaders();

    /**
     * Write a record to the store.
     *
     * @param row Row to write with columns mapped to respective values
     */
    void writeRow(@NonNull Row<T> row);

    /**
     * Close this record source so the resource is no longer in use.
     */
    void close();

    /**
     * Flush the write buffer.
     */
    void flush();

    /**
     * Gets the target file name.
     *
     * @return Name of target file
     */
    String getTargetName();
}
