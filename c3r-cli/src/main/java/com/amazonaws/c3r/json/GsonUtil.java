// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Provides an interface from loading all supported input types as JSON.
 */
public final class GsonUtil {
    /**
     * The specialized context for serialization of schema specifications for C3R classes.
     */
    private static final Gson GSON = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(PadType.class, new PadTypeTypeAdapter())
            .registerTypeAdapter(ColumnType.class, new ColumnTypeTypeAdapter())
            .registerTypeAdapter(ColumnHeader.class, new ColumnHeaderTypeAdapter())
            .registerTypeAdapter(TableSchema.class, new TableSchemaTypeAdapter())
            .registerTypeAdapterFactory(new ValidationTypeAdapterFactory())
            .create();

    /** Hidden utility constructor. */
    private GsonUtil() {
    }

    /**
     * Attempt to parse a string of JSON values to specified class.
     *
     * @param json String containing formatted JSON values
     * @param classOfT Type to parse string as
     * @param <T> Specific class you want from the JSON
     * @return Constructed value (possibly null)
     * @throws C3rIllegalArgumentException If the string can't be parsed as the requested class type
     */
    public static <T> T fromJson(final String json, final Class<T> classOfT) {
        try {
            return GSON.fromJson(json, classOfT);
        } catch (Exception e) {
            throw new C3rIllegalArgumentException("Unable to parse JSON " + classOfT + ".", e);
        }
    }

    /**
     * Converts an object to its representation as a formatted JSON string without specifying a specific class to interpret it as.
     *
     * @param src Object to convert to JSON
     * @return String representing object in pretty printed JSON format
     */
    public static String toJson(final Object src) {
        return toJson(src, null);
    }

    /**
     * Converts an object to its representation as a formatted JSON string with a specific class to interpret it as.
     *
     * @param src Object to convert to JSON
     * @param classOfT Specific class to interpret object as
     * @param <T> Specific class to interpret object as
     * @return String representing the object in pretty printed JSON format
     * @throws C3rIllegalArgumentException If the class could not be serialized to JSON
     */
    public static <T> String toJson(final Object src, final Class<T> classOfT) {
        try {
            if (classOfT == null) {
                return GSON.toJson(src);
            } else {
                return GSON.toJson(src, classOfT);
            }
        } catch (Exception e) {
            throw new C3rIllegalArgumentException("Unable to write " + classOfT + " as JSON.", e);
        }
    }
}
