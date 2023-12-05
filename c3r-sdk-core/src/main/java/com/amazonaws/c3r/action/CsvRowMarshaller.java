// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.CsvValueFactory;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.io.CsvRowWriter;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.io.RowReader;
import com.amazonaws.c3r.io.RowWriter;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used to instantiate an instance of {@link RowMarshaller} that handles CSV data. {@link RowMarshaller} provides all the functionality
 * except for creating the CSV file reader ({@link CsvRowReader}), writer ({@link CsvRowWriter}) and {@link CsvValueFactory} which is done
 * here.
 */
@Slf4j
public final class CsvRowMarshaller {
    /**
     * Utility class, hide default constructor.
     */
    private CsvRowMarshaller() {
    }

    /**
     * Creates an instance of the marshaller based off of an {@link EncryptConfig}. Verifies the input file appears to contain CSV data
     * before continuing.
     *
     * @param config Configuration information on how data will be transformed, file locations, etc.
     * @return CSV data marshaller
     * @throws C3rIllegalArgumentException If non-CSV data was found to be in the file
     * @see EncryptConfig
     */
    public static RowMarshaller<CsvValue> newInstance(@NonNull final EncryptConfig config) {
        if (config.getFileFormat() != FileFormat.CSV) {
            throw new C3rIllegalArgumentException("Expected a CSV encryption configuration, but found "
                    + config.getFileFormat() + " encryption configuration instead.");
        }
        return CsvRowMarshaller.builder()
                .sourceFile(config.getSourceFile())
                .targetFile(config.getTargetFile())
                .tempDir(config.getTempDir())
                .inputNullValue(config.getCsvInputNullValue())
                .outputNullValue(config.getCsvOutputNullValue())
                .settings(config.getSettings())
                .schema(config.getTableSchema())
                .transforms(Transformer.initTransformers(config))
                .build();
    }

    /**
     * Creates an instance of the marshaller where each setting is specified individually.
     *
     * @param sourceFile      Input CSV data file location
     * @param targetFile      Where to write CSV data
     * @param tempDir         Where to write temporary files if needed
     * @param inputNullValue  What the CSV input file uses to indicate {@code null}
     * @param outputNullValue What the output CSV file should use to indicate {@code null}
     * @param settings        Cryptographic settings for the clean room
     * @param schema          Specification of how data in the input file will be transformed into encrypted data in the output file
     * @param transforms      Cryptographic transforms that are possible to use
     * @return CSV data marshaller
     */
    @Builder
    private static RowMarshaller<CsvValue> newInstance(
            @NonNull final String sourceFile,
            @NonNull final String targetFile,
            @NonNull final String tempDir,
            final String inputNullValue,
            final String outputNullValue,
            @NonNull final ClientSettings settings,
            @NonNull final TableSchema schema,
            @NonNull final Map<ColumnType, Transformer> transforms) {
        final RowReader<CsvValue> reader = CsvRowReader.builder().sourceName(sourceFile).inputNullValue(inputNullValue)
                .externalHeaders(schema.getPositionalColumnHeaders()).build();
        final List<ColumnHeader> targetHeaders = schema.getColumns().stream().map(ColumnSchema::getTargetHeader)
                .collect(Collectors.toList());
        final RowWriter<CsvValue> writer = CsvRowWriter.builder()
                .targetName(targetFile)
                .outputNullValue(outputNullValue)
                .headers(targetHeaders)
                .build();
        validate(outputNullValue, schema);
        return RowMarshaller.<CsvValue>builder()
                .settings(settings)
                .schema(schema)
                .tempDir(tempDir)
                .inputReader(reader)
                .valueFactory(new CsvValueFactory())
                .outputWriter(writer)
                .transformers(transforms)
                .build();
    }

    /**
     * Verifies that settings are consistent.
     * - Make sure that if csvOutputNULLValue is set, at least one cleartext target column exists
     *
     * @param outputNullValue What the output CSV file should use to indicate {@code null}
     * @param schema          Specification of how data in the input file will be transformed into encrypted data in the output file
     */
    static void validate(final String outputNullValue, @NonNull final TableSchema schema) {
        if (outputNullValue == null) {
            return; // No custom null value was set
        }
        final boolean cleartextColumnExists = schema.getColumns().stream()
                .anyMatch(columnSchema -> columnSchema.getType() == ColumnType.CLEARTEXT);
        if (!cleartextColumnExists) {
            log.warn("Received a custom output null value `" + outputNullValue + "`, but no cleartext columns were found. It will be " +
                    "ignored.");
        }
    }
}
