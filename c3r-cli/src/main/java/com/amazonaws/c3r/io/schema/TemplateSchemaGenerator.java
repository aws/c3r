// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.json.GsonUtil;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Used to create a simple schema without user input. Creates a one-to-one mapping in the output JSON file which the user can then edit to
 * select the transform and padding types they would like.
 */
@Slf4j
public final class TemplateSchemaGenerator {
    /**
     * String for user-facing messaging showing column type options.
     */
    private static final String COLUMN_TYPE_OPTIONS = "[cleartext|sealed|fingerprint]";

    /**
     * The contents to be printed for each pad in the output, along with instructions on how to use it.
     */
    private static final JsonObject EXAMPLE_PAD;

    static {
        EXAMPLE_PAD = new JsonObject();
        EXAMPLE_PAD.addProperty("COMMENT", "omit this pad entry unless column type is sealed");
        EXAMPLE_PAD.addProperty("type", "[none|fixed|max]");
        EXAMPLE_PAD.addProperty("length", "omit length property for type none, otherwise specify value in [0, 10000]");
    }

    /**
     * Console output stream.
     */
    private final PrintStream consoleOutput;

    /**
     * Names of the columns in the input data.
     */
    private final List<ColumnHeader> headers;

    /**
     * Number of source columns.
     */
    private final int sourceColumnCount;

    /**
     * Source column types (in the order they appear in the input file).
     */
    private final List<ClientDataType> sourceColumnTypes;

    /**
     * Where to write the schema file.
     */
    private final String targetJsonFile;

    /**
     * Initializes the automated schema generator.
     *
     * @param sourceHeaders     List of column names in the input file
     * @param sourceColumnTypes Source column types (in the order they appear in the input file)
     * @param targetJsonFile    Where to write the schema
     * @param consoleOutput     Connection to output stream (i.e., output for user)
     * @throws C3rIllegalArgumentException If input sizes are inconsistent
     */
    @Builder
    private TemplateSchemaGenerator(final List<ColumnHeader> sourceHeaders,
                                    @NonNull final List<ClientDataType> sourceColumnTypes,
                                    @NonNull final String targetJsonFile,
                                    final PrintStream consoleOutput) {
        if (sourceHeaders != null && sourceHeaders.size() != sourceColumnTypes.size()) {
            throw new C3rIllegalArgumentException("Template schema generator given "
                    + sourceHeaders.size() + " headers and " + sourceColumnTypes.size() + " column data types.");
        }
        this.headers = sourceHeaders == null ? null : List.copyOf(sourceHeaders);
        this.sourceColumnTypes = sourceColumnTypes;
        this.sourceColumnCount = sourceColumnTypes.size();
        this.targetJsonFile = targetJsonFile;
        this.consoleOutput = (consoleOutput == null) ? new PrintStream(System.out, true, StandardCharsets.UTF_8)
                : consoleOutput;
    }

    /**
     * Creates template column schemas from the provided (non-{@code null}) source {@code headers}.
     *
     * @return The generated template column schemas
     */
    private JsonArray generateTemplateColumnSchemasFromSourceHeaders() {
        final var columnSchemaArray = new JsonArray(headers.size());
        for (int i = 0; i < sourceColumnCount; i++) {
            final var header = headers.get(i);
            final var entry = new JsonObject();
            entry.addProperty("sourceHeader", header.toString());
            entry.addProperty("targetHeader", header.toString());
            if (sourceColumnTypes.get(i).supportsCryptographicComputing()) {
                entry.addProperty("type", COLUMN_TYPE_OPTIONS);
                entry.add("pad", EXAMPLE_PAD);
            } else {
                consoleOutput.println(SchemaGeneratorUtils.unsupportedTypeWarning(header, i));
                entry.addProperty("type", "cleartext");
            }
            columnSchemaArray.add(entry);
        }
        return columnSchemaArray;
    }

    /**
     * Creates template column schemas for headerless source.
     *
     * @return The generated template column schemas
     */
    private JsonArray generateTemplateColumnSchemasFromColumnCount() {
        final var columnSchemaArray = new JsonArray(sourceColumnCount);
        for (int i = 0; i < sourceColumnCount; i++) {
            final var entry = new JsonObject();
            entry.addProperty("targetHeader", ColumnHeader.getColumnHeaderFromIndex(i).toString());
            if (sourceColumnTypes.get(i).equals(ClientDataType.STRING)) {
                entry.addProperty("type", COLUMN_TYPE_OPTIONS);
                entry.add("pad", EXAMPLE_PAD);
            } else {
                entry.addProperty("type", "cleartext");
            }
            final var entryArray = new JsonArray(1);
            entryArray.add(entry);
            columnSchemaArray.add(entryArray);
        }
        return columnSchemaArray;
    }

    /**
     * Generate a template schema. I.e., the type (see {@link com.amazonaws.c3r.config.ColumnType}) and padding
     * (see {@link com.amazonaws.c3r.config.PadType}) are left with all possible options and must be manually edited.
     *
     * @throws C3rRuntimeException If unable to write to the target file
     */
    public void run() {
        final var schemaContent = new JsonObject();
        if (headers != null) {
            schemaContent.addProperty("headerRow", true);
            schemaContent.add("columns", generateTemplateColumnSchemasFromSourceHeaders());
        } else {
            schemaContent.addProperty("headerRow", false);
            schemaContent.add("columns", generateTemplateColumnSchemasFromColumnCount());
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(targetJsonFile), StandardCharsets.UTF_8)) {
            writer.write(GsonUtil.toJson(schemaContent));
        } catch (IOException e) {
            throw new C3rRuntimeException("Could not write to target schema file.", e);
        }
        log.info("Template schema written to {}.", targetJsonFile);
        log.info("Schema requires manual modification before use:");
        log.info("  * Types for each column must be selected.");
        log.info("  * Pad entry must be modified for each sealed column and removed for other column types.");
        log.info("Resulting schema must be valid JSON (e.g., final entries in objects have no trailing comma, etc).");
    }
}
