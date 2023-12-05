// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.ParquetValueFactory;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.io.ParquetRowReader;
import com.amazonaws.c3r.io.ParquetRowWriter;
import lombok.Builder;
import lombok.NonNull;

import java.util.Map;

/**
 * Used to instantiate an instance of {@link RowUnmarshaller} that handles Parquet data. {@link RowUnmarshaller} provides all the
 * functionality except for creating the Parquet file reader ({@link ParquetRowReader}), writer ({@link ParquetRowWriter}) and
 * {@link ParquetValueFactory} which is done here.
 */
public final class ParquetRowUnmarshaller {
    /**
     * Utility class, hide default constructor.
     */
    private ParquetRowUnmarshaller() {
    }

    /**
     * Creates an instance of the marshaller based off of an {@link DecryptConfig}. Verifies the input file appears to contain Parquet data
     * before continuing.
     *
     * @param config Configuration information on how data will be transformed, file locations, etc.
     * @return Parquet data unmarshaller
     * @throws C3rIllegalArgumentException If non-Parquet data was found to be in the file
     * @see DecryptConfig
     */
    public static RowUnmarshaller<ParquetValue> newInstance(@NonNull final DecryptConfig config) {
        if (config.getFileFormat() != FileFormat.PARQUET) {
            throw new C3rIllegalArgumentException("Expected a PARQUET decryption configuration, but found "
                    + config.getFileFormat() + ".");
        }

        return ParquetRowUnmarshaller.builder()
                .sourceFile(config.getSourceFile())
                .targetFile(config.getTargetFile())
                .transformers(Transformer.initTransformers(config))
                .build();
    }

    /**
     * Creates an instance of a Parquet row unmarshaller based off of individually specified settings.
     *
     * @param sourceFile   Input Parquet file location
     * @param targetFile   Where to write Parquet data
     * @param transformers Cryptographic transforms that are possible to use
     * @return Parquet data unmarshaller
     */
    @Builder
    private static RowUnmarshaller<ParquetValue> newInstance(
            @NonNull final String sourceFile,
            @NonNull final String targetFile,
            @NonNull final Map<ColumnType, Transformer> transformers) {
        final ParquetRowReader reader = ParquetRowReader.builder()
                .sourceName(sourceFile)
                .skipHeaderNormalization(true)
                .build();
        final var parquetSchema = reader.getParquetSchema();
        final ParquetRowWriter writer = ParquetRowWriter.builder()
                .targetName(targetFile)
                .parquetSchema(parquetSchema)
                .build();

        return RowUnmarshaller.<ParquetValue>builder()
                .inputReader(reader)
                .valueFactory(new ParquetValueFactory(parquetSchema.getColumnParquetDataTypeMap()))
                .outputWriter(writer)
                .transformers(transformers)
                .build();
    }
}
