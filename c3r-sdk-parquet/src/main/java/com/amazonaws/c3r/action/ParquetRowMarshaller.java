// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ParquetRowFactory;
import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.io.ParquetRowReader;
import com.amazonaws.c3r.io.ParquetRowWriter;
import lombok.Builder;
import lombok.NonNull;

import java.util.Map;

/**
 * Used to instantiate an instance of {@link RowMarshaller} that handles Parquet data. {@link RowMarshaller} provides all the functionality
 * except for creating the Parquet file reader ({@link ParquetRowReader}), writer ({@link ParquetRowWriter}) and {@link ParquetRowFactory}
 * which is done here.
 */
public final class ParquetRowMarshaller {
    /**
     * Utility class, hide default constructor.
     */
    private ParquetRowMarshaller() {
    }

    /**
     * Creates an instance of the marshaller based off of an {@link EncryptConfig}. Verifies the input file appears to contain Parquet data
     * before continuing.
     *
     * @param config Configuration information on how data will be transformed, file locations, etc.
     * @return Parquet data marshaller
     * @throws C3rIllegalArgumentException If non-Parquet data was found to be in the file
     * @see EncryptConfig
     */
    public static RowMarshaller<ParquetValue> newInstance(@NonNull final EncryptConfig config) {
        if (config.getFileFormat() != FileFormat.PARQUET) {
            throw new C3rIllegalArgumentException("Expected a PARQUET encryption configuration, but found "
                    + config.getFileFormat() + ".");
        }

        return ParquetRowMarshaller.builder()
                .sourceFile(config.getSourceFile())
                .targetFile(config.getTargetFile())
                .settings(config.getSettings())
                .schema(config.getTableSchema())
                .tempDir(config.getTempDir())
                .transforms(Transformer.initTransformers(config))
                .build();
    }


    /**
     * Creates an instance of the marshaller where each setting is specified individually via {@link Builder}.
     *
     * @param sourceFile Input Parquet data file location
     * @param targetFile Where to write Parquet data
     * @param tempDir    Where to write temporary files if needed
     * @param settings   Cryptographic settings for the clean room
     * @param schema     Specification of how data in the input file will be transformed into encrypted data in the output file
     * @param transforms Cryptographic transforms that are possible to use
     * @return Parquet data marshaller
     * @throws C3rIllegalArgumentException If given a non-mapped table schema
     */
    @Builder
    private static RowMarshaller<ParquetValue> newInstance(
            @NonNull final String sourceFile,
            @NonNull final String targetFile,
            @NonNull final String tempDir,
            @NonNull final ClientSettings settings,
            @NonNull final TableSchema schema,
            @NonNull final Map<ColumnType, Transformer> transforms) {
        if (schema.getPositionalColumnHeaders() != null) {
            throw new C3rIllegalArgumentException("Parquet files require a mapped table schema.");
        }
        final ParquetRowReader reader = new ParquetRowReader(sourceFile);
        final ParquetSchema sourceParquetSchema = reader.getParquetSchema();
        final ParquetSchema targetParquetSchema = sourceParquetSchema.deriveTargetSchema(schema);
        final ParquetRowWriter writer = ParquetRowWriter.builder()
                .targetName(targetFile)
                .parquetSchema(targetParquetSchema)
                .build();

        return RowMarshaller.<ParquetValue>builder()
                .settings(settings)
                .schema(schema)
                .tempDir(tempDir)
                .inputReader(reader)
                .rowFactory(new ParquetRowFactory(targetParquetSchema.getColumnParquetDataTypeMap()))
                .outputWriter(writer)
                .transformers(transforms)
                .build();
    }
}
