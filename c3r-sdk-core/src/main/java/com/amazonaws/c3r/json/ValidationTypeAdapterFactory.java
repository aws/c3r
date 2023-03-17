// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Validatable;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Ensures {@link Validatable} objects are checked for validity after being deserialized.
 */
public class ValidationTypeAdapterFactory implements TypeAdapterFactory {
    /**
     * Creates an instance of an object that implements the {@link Validatable} interface and calls validate to verify correct
     * construction of object according to C3R type rules. A number of classes implement this interface and are constructed automatically
     * or via a custom {@link TypeAdapter}, then pass through this factory where the constructed object is checked for correctness. This
     * particular factory only changes the read process, nothing is done during the write step on top of the class's normal write call.
     *
     * @param <T> Specific type being constructed or written
     * @param gson JSON parser with customized adapters
     * @param type Higher level type
     * @return Type factory that calls {@link Validatable#validate()} on all implementing classes when reading in JSON
     */
    @Override
    public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> type) {
        final TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

        return new TypeAdapter<>() {
            public void write(final JsonWriter out, final T value) {
                try {
                    delegate.write(out, value);
                } catch (IOException e) {
                    throw new C3rRuntimeException("Unable to write to output.", e);
                }
            }

            public T read(final JsonReader in) {
                try {
                    final T value = delegate.read(in);
                    if (value instanceof Validatable) {
                        ((Validatable) value).validate();
                    }
                    return value;
                } catch (IOException e) {
                    throw new C3rRuntimeException("Unable to read from the input.", e);
                }
            }
        };
    }
}

