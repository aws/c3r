// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.schema;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

/**
 * Used to generate a schema file for a specific CSV file. User can ask for either a simple, autogenerated schema or be walked through
 * the entire schema creation process.
 */
@Slf4j
public final class CsvSchemaGenerator extends SchemaGenerator {
    /**
     * How many columns are in the source file.
     */
    @Getter
    private final int sourceColumnCount;

    /**
     * CSV file to generate a schema for.
     */
    private final String inputCsvFile;

    /**
     * Schema file location.
     */
    private final String targetJsonFile;

    /**
     * Set up for schema generation and validate settings.
     *
     * @param inputCsvFile   CSV file to read header information from
     * @param targetJsonFile Where to save the schema
     * @param overwrite      If the {@code targetJsonFile} should be overwritten if it exists
     * @param hasHeaders     Does the first source row contain column headers?
     * @param clientSettings Collaboration's client settings if provided, else {@code null}
     */
    @Builder
    private CsvSchemaGenerator(@NonNull final String inputCsvFile,
                               @NonNull final String targetJsonFile,
                               @NonNull final Boolean overwrite,
                               @NonNull final Boolean hasHeaders,
                               final ClientSettings clientSettings) {
        super(inputCsvFile, targetJsonFile, overwrite, clientSettings);
        this.inputCsvFile = inputCsvFile;
        this.targetJsonFile = targetJsonFile;
        FileUtil.initFileIfNotExists(targetJsonFile);

        if (hasHeaders) {
            final CsvRowReader reader = CsvRowReader.builder()
                    .sourceName(inputCsvFile)
                    .build();
            sourceHeaders = reader.getHeaders();
            sourceColumnCount = sourceHeaders.size();
            reader.close();
        } else {
            sourceColumnCount = CsvRowReader.getCsvColumnCount(inputCsvFile, null);
            sourceHeaders = null;
        }
        this.sourceColumnTypes = Collections.nCopies(sourceColumnCount, CsvValue.CLIENT_DATA_TYPE);
    }
}