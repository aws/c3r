// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Handles the serialization/deserialization of ColumnTypes. Allows for case-insensitivity.
 */
public class ColumnTypeTypeAdapter extends TypeAdapter<ColumnType> {
    /**
     * Serialize {@link ColumnType} object to a string and send to {@code out}.
     *
     * @param out Stream of values written so far
     * @param value the Java object to write
     * @throws C3rRuntimeException If there's an error writing to output
     */
    @Override
    public void write(final JsonWriter out, final ColumnType value) {
        try {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.toString());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Error writing to output.", e);
        }
    }

    /**
     * Read in a JSON value and attempt to deserialize it as a {@link ColumnType}.
     *
     * @param in Stream of tokenized JSON values
     * @return Type of column transform to use
     * @throws C3rRuntimeException If there's an error reading from source
     */
    @Override
    public ColumnType read(final JsonReader in) {
        try {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            } else {
                return ColumnType.valueOf(in.nextString().trim().toUpperCase());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Error reading from input.", e);
        }
    }
}
