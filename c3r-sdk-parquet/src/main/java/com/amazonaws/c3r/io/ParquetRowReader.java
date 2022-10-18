// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetRowFactory;
import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.parquet.ParquetRowMaterializer;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Specific implementation for reading Parquet files in preparation for data marshalling.
 */
public final class ParquetRowReader extends RowReader<ParquetValue> {
    /**
     * Maximum number of columns allowed.
     *
     * <p>
     * This is defined at the implementation layer and not the RowReader interface in order to allow tuning among different formats.
     */
    static final int MAX_COLUMN_COUNT = 1600;

    /**
     * Name of input file.
     */
    private final String sourceName;

    /**
     * Reads Parquet files from disk.
     */
    private final ParquetFileReader fileReader;

    /**
     * Data types for each column along with other metadata for Parquet.
     *
     * @see ParquetDataType
     */
    @Getter
    private final ParquetSchema parquetSchema;

    /**
     * Creates an empty row for Parquet data to be written in to.
     */
    private final ParquetRowFactory rowFactory;

    /**
     * Headers for columns in the Parquet file.
     */
    private PageReadStore rowGroup;

    /**
     * Size of the largest row group encountered while reading.
     */
    @Getter
    private int maxRowGroupSize;

    /**
     * Number of rows remaining in current batch.
     */
    private long rowsLeftInGroup;

    /**
     * Handles reading Parquet data from some data source.
     */
    private RecordReader<Row<ParquetValue>> rowReader;

    /**
     * Whether the reader has been closed.
     */
    private boolean closed;

    /**
     * Data in the next row.
     */
    private Row<ParquetValue> nextRow;

    /**
     * Creates a record reader for a Parquet file.
     *
     * @param sourceName Path to be read as a Parquet file path
     * @throws C3rRuntimeException If {@code fileName} cannot be opened for reading
     */
    public ParquetRowReader(@NonNull final String sourceName) {
        this.sourceName = sourceName;
        final var conf = new org.apache.hadoop.conf.Configuration();
        final org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(sourceName);

        try {
            fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf));
        } catch (FileNotFoundException e) {
            throw new C3rRuntimeException("Unable to find file " + sourceName + ".", e);
        } catch (IOException | RuntimeException e) {
            throw new C3rRuntimeException("Error reading from file " + sourceName + ".", e);
        }
        parquetSchema = new ParquetSchema(fileReader.getFooter().getFileMetaData().getSchema());
        if (parquetSchema.getHeaders().size() > MAX_COLUMN_COUNT) {
            throw new C3rRuntimeException("Couldn't parse input file. Please verify that column count does not exceed " + MAX_COLUMN_COUNT
                    + ".");
        }
        final Map<ColumnHeader, ParquetDataType> columnTypeMap = parquetSchema.getHeaders().stream()
                .collect(Collectors.toMap(Function.identity(), parquetSchema::getColumnType));

        rowFactory = new ParquetRowFactory(columnTypeMap);

        refreshNextRow();
    }

    /**
     * Get the column headers.
     *
     * @return List of column headers
     */
    public List<ColumnHeader> getHeaders() {
        return parquetSchema.getHeaders();
    }

    /**
     * Gets the next row out of the Parquet file along with the associated schema.
     *
     * @throws C3rRuntimeException If the next row group can't be read
     */
    private void loadNextRowGroup() {
        try {
            rowGroup = fileReader.readNextRowGroup();
        } catch (IOException e) {
            throw new C3rRuntimeException("Error while reading row group from " + fileReader.getFile(), e);
        }
        if (rowGroup != null) {
            rowsLeftInGroup = rowGroup.getRowCount();
            maxRowGroupSize = Math.max(maxRowGroupSize,
                    rowsLeftInGroup > Integer.MAX_VALUE ? Integer.MAX_VALUE : Long.valueOf(rowsLeftInGroup).intValue());
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema.getMessageType());
            rowReader = columnIO.getRecordReader(rowGroup, new ParquetRowMaterializer(parquetSchema, rowFactory));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!closed) {
            try {
                fileReader.close();
            } catch (IOException e) {
                throw new C3rRuntimeException("Unable to close connection to file.", e);
            }
            closed = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void refreshNextRow() {
        if (rowsLeftInGroup <= 0) {
            loadNextRowGroup();
        }
        if (rowGroup != null && rowsLeftInGroup > 0) {
            nextRow = rowReader.read();
            rowsLeftInGroup--;
        } else {
            nextRow = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Row<ParquetValue> peekNextRow() {
        return nextRow != null ? nextRow.clone() : null;
    }

    /**
     * Describes where rows are being read from in a human-friendly fashion.
     *
     * @return Name of source being used
     */
    @Override
    public String getSourceName() {
        return sourceName;
    }
}