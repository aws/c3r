// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Handles the serialization/deserialization of PadTypes. Allows for case-insensitivity.
 */
public class PadTypeTypeAdapter extends TypeAdapter<PadType> {
    /**
     * Serialize {@link PadType} object to a string and send to {@code out}.
     *
     * @param out Output stream of formatted JSON data
     * @param value Pad type
     * @throws C3rRuntimeException If there's an error writing to output
     */
    @Override
    public void write(final JsonWriter out, final PadType value) {
        try {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.name());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Unable to write to output.", e);
        }
    }

    /**
     * Read in a JSON value and attempt to deserialize it as a {@link PadType}.
     *
     * @param in Input stream of tokenized JSON
     * @return Pad type specified
     * @throws C3rRuntimeException If there's an error reading from source
     */
    @Override
    public PadType read(final JsonReader in) {
        try {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            } else {
                return PadType.valueOf(in.nextString().toUpperCase());
            }
        } catch (IOException e) {
            throw new C3rRuntimeException("Error reading from input.", e);
        }
    }
}
