// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.data.CsvRowFactory;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.io.CsvRowWriter;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.io.RowReader;
import com.amazonaws.c3r.io.RowWriter;
import lombok.Builder;
import lombok.NonNull;

import java.util.Map;

/**
 * Used to instantiate an instance of {@link RowUnmarshaller} that handles CSV data. {@link RowUnmarshaller} provides all the functionality
 * except for creating the CSV file reader ({@link CsvRowReader}), writer ({@link CsvRowWriter}) and {@link CsvRowFactory} which is done
 * here.
 */
public final class CsvRowUnmarshaller {
    /**
     * Utility class, hide default constructor.
     */
    private CsvRowUnmarshaller() {
    }

    /**
     * Creates an instance of the marshaller based off of an {@link DecryptConfig}. Verifies the input file appears to contain CSV data
     * before continuing.
     *
     * @param config Configuration information on how data will be transformed, file locations, etc.
     * @return CSV data unmarshaller
     * @throws C3rIllegalArgumentException If non-CSV data was found to be in the file
     * @see DecryptConfig
     */
    public static RowUnmarshaller<CsvValue> newInstance(@NonNull final DecryptConfig config) {
        if (config.getFileFormat() != FileFormat.CSV) {
            throw new C3rIllegalArgumentException("Expected a CSV decryption configuration, but found "
                    + config.getFileFormat() + " decryption configuration instead.");
        }

        return CsvRowUnmarshaller.builder()
                .sourceFile(config.getSourceFile())
                .targetFile(config.getTargetFile())
                .csvInputNullValue(config.getCsvInputNullValue())
                .csvOutputNullValue(config.getCsvOutputNullValue())
                .transformers(Transformer.initTransformers(config))
                .build();
    }

    /**
     * Creates an instance of a CSV row unmarshaller based off of individually specified settings.
     *
     * @param sourceFile Input CSV file location
     * @param targetFile Where to write CSV data
     * @param csvInputNullValue What the CSV input file uses to indicate {@code null}
     * @param csvOutputNullValue What the output CSV file should use to indicate {@code null}
     * @param transformers Cryptographic transforms that are possible to use
     * @return CSV data unmarshaller
     */
    @Builder
    private static RowUnmarshaller<CsvValue> newInstance(
            @NonNull final String sourceFile,
            @NonNull final String targetFile,
            final String csvInputNullValue,
            final String csvOutputNullValue,
            @NonNull final Map<ColumnType, Transformer> transformers) {
        final RowReader<CsvValue> reader = CsvRowReader.builder().sourceName(sourceFile)
                .inputNullValue(csvInputNullValue).build();
        final RowWriter<CsvValue> writer = CsvRowWriter.builder()
                .targetName(targetFile)
                .outputNullValue(csvOutputNullValue)
                .headers(reader.getHeaders())
                .build();

        return RowUnmarshaller.<CsvValue>builder()
                .inputReader(reader)
                .rowFactory(new CsvRowFactory())
                .outputWriter(writer)
                .transformers(transformers)
                .build();
    }
}
