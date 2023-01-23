// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnType;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to create a simple schema without user input. Creates a one-to-one mapping in the output JSON file which the user can then edit to
 * select the transform and padding types they would like.
 */
@Slf4j
public final class TemplateSchemaGenerator {

    /**
     * String for user-facing messaging showing column type options.
     */
    private static final String ALL_COLUMN_TYPES = "[" +
            Arrays.stream(ColumnType.values())
                    .map(ColumnType::toString)
                    .collect(Collectors.joining("|")) +
            "]";

    /**
     * String for user-facing messaging showing column type options.
     */
    private static final String ALL_COLUMN_TYPES_SANS_CLEARTEXT = "[" +
            Arrays.stream(ColumnType.values())
                    .filter(c -> c != ColumnType.CLEARTEXT)
                    .map(ColumnType::toString)
                    .collect(Collectors.joining("|")) +
            "]";

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
     * Options for column types based on ClientSettings (if provided).
     */
    private final String columnTypeOptions;

    /**
     * Whether this schema can have cleartext columns.
     */
    private final boolean allowCleartextColumns;

    /**
     * Initializes the automated schema generator.
     *
     * @param sourceHeaders     List of column names in the input file
     * @param sourceColumnTypes Source column types (in the order they appear in the input file)
     * @param targetJsonFile    Where to write the schema
     * @param consoleOutput     Connection to output stream (i.e., output for user)
     * @param clientSettings    Collaboration's client settings if provided, else {@code null}
     * @throws C3rIllegalArgumentException If input sizes are inconsistent
     */
    @Builder
    private TemplateSchemaGenerator(final List<ColumnHeader> sourceHeaders,
                                    @NonNull final List<ClientDataType> sourceColumnTypes,
                                    @NonNull final String targetJsonFile,
                                    final PrintStream consoleOutput,
                                    final ClientSettings clientSettings) {
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
        allowCleartextColumns = clientSettings == null || clientSettings.isAllowCleartext();
        if (allowCleartextColumns) {
            columnTypeOptions = ALL_COLUMN_TYPES;
        } else {
            columnTypeOptions = ALL_COLUMN_TYPES_SANS_CLEARTEXT;
        }
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
                entry.addProperty("type", columnTypeOptions);
                entry.add("pad", EXAMPLE_PAD);
            } else if (allowCleartextColumns) {
                consoleOutput.println(SchemaGeneratorUtils.unsupportedTypeWarning(header, i));
                entry.addProperty("type", ColumnType.CLEARTEXT.toString());
            } else {
                consoleOutput.println(SchemaGeneratorUtils.unsupportedTypeSkippingColumnWarning(header, i));
                continue;
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
            // Array template entry will go in
            final var entryArray = new JsonArray(1);
            // template entry
            final var templateEntry = new JsonObject();
            templateEntry.addProperty("targetHeader", ColumnHeader.getColumnHeaderFromIndex(i).toString());
            if (sourceColumnTypes.get(i).supportsCryptographicComputing()) {
                templateEntry.addProperty("type", columnTypeOptions);
                templateEntry.add("pad", EXAMPLE_PAD);
                entryArray.add(templateEntry);
            } else if (allowCleartextColumns) {
                templateEntry.addProperty("type", ColumnType.CLEARTEXT.toString());
                entryArray.add(templateEntry);
            } else {
                // If the column type does not support cryptographic computing and cleartext columns are not allowed,
                // then we do not add a template entry to the array, and we warn the user this column has been skipped.
                consoleOutput.println(SchemaGeneratorUtils.unsupportedTypeSkippingColumnWarning(null, i));
            }
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
