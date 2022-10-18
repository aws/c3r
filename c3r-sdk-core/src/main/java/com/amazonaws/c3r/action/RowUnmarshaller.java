// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.RowFactory;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.RowReader;
import com.amazonaws.c3r.io.RowWriter;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;

/**
 * Interface for loading and processing records for decryption.
 *
 * @param <T> Data format
 * @see RowReader
 * @see RowWriter
 */
@Slf4j
public final class RowUnmarshaller<T extends Value> {
    /**
     * Used for reading a row of data.
     */
    @Getter
    private final RowReader<T> inputReader;

    /**
     * Used for writing a row of data.
     */
    @Getter
    private final RowWriter<T> outputWriter;

    /**
     * Creates an empty row to be filled with data.
     */
    private final RowFactory<T> rowFactory;

    /**
     * Cryptographic computation transformers in use for data processing.
     */
    private final Map<ColumnType, Transformer> transformers;

    /**
     * Creates a {@code RowUnmarshaller}. See {@link #unmarshal} for primary functionality.
     *
     * @param rowFactory   Creates new rows for writing out unmarshalled data
     * @param inputReader  Where marshalled records are read from
     * @param outputWriter Where unmarshalled records are written to
     * @param transformers The transformers for unmarshalling data
     */
    @Builder(access = AccessLevel.PACKAGE)
    private RowUnmarshaller(@NonNull final RowFactory<T> rowFactory,
                            @NonNull final RowReader<T> inputReader,
                            @NonNull final RowWriter<T> outputWriter,
                            @NonNull final Map<ColumnType, Transformer> transformers) {
        this.rowFactory = rowFactory;
        this.inputReader = inputReader;
        this.outputWriter = outputWriter;
        this.transformers = Collections.unmodifiableMap(transformers);
    }

    /**
     * Reads in records/rows from the given source and outputs it to the given target after applying cryptographic transforms.
     *
     * @throws C3rRuntimeException If there's an error during decryption
     */
    public void unmarshal() {
        while (inputReader.hasNext()) {
            final Row<T> marshalledRow = inputReader.next();
            final Row<T> unmarshalledRow = rowFactory.newRow();
            for (ColumnHeader header : marshalledRow.getHeaders()) {
                final byte[] valueBytes = marshalledRow.getValue(header).getBytes();
                Transformer transformer = transformers.get(ColumnType.CLEARTEXT); // Default to pass through
                if (Transformer.hasDescriptor(transformers.get(ColumnType.SEALED), valueBytes)) {
                    transformer = transformers.get(ColumnType.SEALED);
                } else if (Transformer.hasDescriptor(transformers.get(ColumnType.FINGERPRINT), valueBytes)) {
                    transformer = transformers.get(ColumnType.FINGERPRINT);
                }
                try {
                    unmarshalledRow.putBytes(
                            header,
                            transformer.unmarshal(valueBytes));
                } catch (Exception e) {
                    throw new C3rRuntimeException("Failed while unmarshalling data for column `"
                            + header + "` on row " + inputReader.getReadRowCount() + ". Error message received: " + e.getMessage(), e);
                }
            }
            outputWriter.writeRow(unmarshalledRow);
            if (inputReader.getReadRowCount() % RowMarshaller.LOG_ROW_UPDATE_FREQUENCY == 0) {
                log.info("{} rows decrypted.", inputReader.getReadRowCount());
            }
        }
        outputWriter.flush();
    }

    /**
     * Closes connections to input source and output target.
     */
    public void close() {
        inputReader.close();
        outputWriter.close();
    }
}
