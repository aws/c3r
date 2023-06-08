// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.schema;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.spark.cli.SchemaMode;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Helps generate a schema for a file with a supported data format.
 */
public abstract class SchemaGenerator {
    /**
     * The headers for the source file, or {@code null} if the file has none.
     */
    @Getter
    protected List<ColumnHeader> sourceHeaders;

    /**
     * Column types for the source file.
     */
    @Getter
    protected List<ClientDataType> sourceColumnTypes;

    /**
     * The file a schema will be generated for.
     */
    private final String inputFile;

    /**
     * The location the generated schema will be stored.
     */
    private final String targetJsonFile;

    /**
     * Clean room cryptographic settings.
     */
    private final ClientSettings clientSettings;

    /**
     * Setup common schema generator component.
     *
     * @param inputFile      Input data file for processing
     * @param targetJsonFile Schema file mapping input to output file data
     * @param overwrite      Whether to overwrite the output file if it already exists
     * @param clientSettings Collaboration settings if available, else {@code null}
     */
    protected SchemaGenerator(@NonNull final String inputFile,
                              @NonNull final String targetJsonFile,
                              @NonNull final Boolean overwrite,
                              final ClientSettings clientSettings) {
        this.inputFile = inputFile;
        this.targetJsonFile = targetJsonFile;
        validate(overwrite);
        FileUtil.initFileIfNotExists(targetJsonFile);
        this.clientSettings = clientSettings;
    }

    /**
     * Verifies that input and target files have appropriate permissions.
     *
     * @param overwrite If the target JSON file can overwrite an existing file
     */
    private void validate(final boolean overwrite) {
        FileUtil.verifyReadableFile(inputFile);
        FileUtil.verifyWritableFile(targetJsonFile, overwrite);
    }

    /**
     * Generate a schema file.
     *
     * @param subMode How the schema file should be generated
     * @throws C3rIllegalArgumentException If the schema generation mode is invalid
     */
    public void generateSchema(final SchemaMode.SubMode subMode) {
        // CHECKSTYLE:OFF
        System.out.println();
        System.out.println("A schema file will be generated for file " + inputFile + ".");
        // CHECKSTYLE:ON
        if (subMode.isInteractiveMode()) {
            InteractiveSchemaGenerator.builder()
                    .sourceHeaders(getSourceHeaders())
                    .sourceColumnTypes(getSourceColumnTypes())
                    .targetJsonFile(targetJsonFile)
                    .consoleInput(new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)))
                    .consoleOutput(System.out)
                    .clientSettings(clientSettings)
                    .build()
                    .run();
        } else if (subMode.isTemplateMode()) {
            TemplateSchemaGenerator.builder()
                    .sourceHeaders(getSourceHeaders())
                    .sourceColumnTypes(getSourceColumnTypes())
                    .targetJsonFile(targetJsonFile)
                    .clientSettings(clientSettings)
                    .build()
                    .run();
        } else {
            throw new C3rIllegalArgumentException("Schema generation mode must be interactive or template.");
        }
    }
}