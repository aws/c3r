// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Handles the serialization/deserialization of ColumnHeader names. Allows for case-insensitivity.
 */
public class ColumnHeaderTypeAdapter extends TypeAdapter<ColumnHeader> {
    /**
     * Serialize {@link ColumnHeader} object to a string and send to {@code out}.
     *
     * @param out Formatted JSON output to add to
     * @param value The column header name to write
     * @throws C3rRuntimeException If there's an error writing to output
     */
    @Override
    public void write(final JsonWriter out, final ColumnHeader value) {
        try {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.toString());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Error writing to output", e);
        }
    }

    /**
     * Read in a JSON value and attempt to deserialize it as a {@link ColumnHeader}.
     *
     * @param in Source to read value from
     * @return The value parsed as a header name
     * @throws C3rRuntimeException If there's an error reading from source
     */
    @Override
    public ColumnHeader read(final JsonReader in) {
        in.setLenient(true);
        try {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            } else {
                return new ColumnHeader(in.nextString());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Error reading from input", e);
        }
    }
}
