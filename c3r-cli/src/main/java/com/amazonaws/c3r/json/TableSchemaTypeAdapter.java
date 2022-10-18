// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Helper class to determine if JSON being serialized/deserialized is a Mapped or Positional TableSchema.
 */
public class TableSchemaTypeAdapter implements JsonDeserializer<TableSchema>, JsonSerializer<TableSchema> {
    /**
     * Confirms the requested class type matches a type supported by this adapter.
     *
     * @param typeOfT Requested object type
     * @throws C3rIllegalArgumentException If {@code typeOfT} is not support by this adapter
     */
    private void checkClassesMatch(final Type typeOfT) {
        // Make sure we're deserializing a TableSchema
        if (typeOfT != TableSchema.class && typeOfT != MappedTableSchema.class && typeOfT != PositionalTableSchema.class) {
            throw new C3rIllegalArgumentException("Expected class type " + typeOfT.getTypeName() + " is not supported by TableSchema.");
        }
    }

    /**
     * Ensures proper format of the object and gets the {@code headerRow} field from the object.
     *
     * @param jsonObject {@code TableSchema} or child being deserialized
     * @return Boolean value stored in {@code JsonObject}
     * @throws C3rRuntimeException If headerRow is null or headerRow is not a boolean
     */
    private static boolean getHasHeaderRow(final JsonObject jsonObject) {
        // Get property headerRow, make sure it exists and is a boolean value
        final JsonElement headerRow = jsonObject.get("headerRow");
        if (headerRow == null || !headerRow.isJsonPrimitive() || !headerRow.getAsJsonPrimitive().isBoolean()) {
            throw new C3rRuntimeException("JSON object should contain boolean value headerRow");
        }

        // Return value of headerRow
        return headerRow.getAsBoolean();
    }

    /**
     * Read in a JSON value and attempt to deserialize it as a {@link TableSchema} child class.
     *
     * <p>
     * This class determines which underlying implementation of TableSchema should be used for deserialization
     *
     * @param json    The Json data being deserialized
     * @param typeOfT Class to deserialize to (should be {@code TableSchema}
     * @param context Helper for deserializing fields and subclasses
     * @return A {@code TableSchema} that is backed by either a {@code MappedTableSchema} or {@code PositionalTableSchema}
     * @throws C3rRuntimeException If json is not in the expected format of a {@code TableSchema} child class
     */
    @Override
    public TableSchema deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) {
        // Confirm this is a supported type
        checkClassesMatch(typeOfT);

        // Make sure we're deserializing an object
        if (!json.isJsonObject()) {
            throw new C3rRuntimeException("TableSchema expects a JSON Object at this point of deserialization.");
        }

        // Get headers and columns with formatting checks
        final JsonObject jsonObject = json.getAsJsonObject();
        final boolean hasHeaderRow = getHasHeaderRow(jsonObject);

        // Construct child class based on if it's mapped or positional
        if (hasHeaderRow) {
            return context.deserialize(json, MappedTableSchema.class);
        } else {
            return context.deserialize(json, PositionalTableSchema.class);
        }
    }

    /**
     * Serialize {@link TableSchema} object into a {@link JsonElement}.
     *
     * <p>
     * This gets called when we specify the class while calling serialize, otherwise JSON will move to the auto-generated
     * serializers for the child implementations.
     *
     * @param src       the object that needs to be converted to Json
     * @param typeOfSrc the actual type (fully generalized version) of the source object
     * @param context Serialization context
     * @return a {@code JsonElement} corresponding to the specified object.
     * @throws C3rIllegalArgumentException If the object passed in is not a child class of a {@code TableSchema}
     */
    @Override
    public JsonElement serialize(final TableSchema src, final Type typeOfSrc, final JsonSerializationContext context) {
        // Make sure the source type matches this class
        checkClassesMatch(typeOfSrc);

        // Dispatch to the correct underlying implementation
        if (src.getClass() == MappedTableSchema.class) {
            return context.serialize(src, MappedTableSchema.class);
        } else if (src.getClass() == PositionalTableSchema.class) {
            return context.serialize(src, PositionalTableSchema.class);
        } else {
            // Paranoia check: we have added something that inherits TableSchema but not routed it for serialization correctly
            throw new C3rIllegalArgumentException("Expected child class of TableSchema but found " + typeOfSrc.getTypeName() + ".");
        }
    }
}
