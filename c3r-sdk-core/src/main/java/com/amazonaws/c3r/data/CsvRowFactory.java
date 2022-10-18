// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

/**
 * Factory for creating empty CSV rows.
 */
public class CsvRowFactory implements RowFactory<CsvValue> {
    /**
     * Creates an empty CSV row for storing data.
     *
     * @return Empty CSV value row
     */
    public CsvRow newRow() {
        return new CsvRow();
    }
}
