// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Resource for writing record entries to an underlying store for processing.
 */
public final class CsvRowWriter implements RowWriter<CsvValue> {
    /**
     * Where to write CSV data.
     */
    @Getter
    private final String targetName;

    /**
     * Column names for the output CSV file.
     */
    @Getter
    private final List<ColumnHeader> headers;

    /**
     * Interprets and writes CSV data to target.
     */
    private final CsvWriter writer;

    /**
     * Creates a record reader for a CSV file.
     *
     * @param targetName      File name for CSV to be written to
     * @param outputNullValue The value representing NULL in the output file. By default, `,,` will represent NULL
     * @param headers         The headers of the individual columns in the output CSV
     * @param fileCharset     Character set of the file. Defaults to {@code UTF_8} if {@code null}
     * @throws C3rRuntimeException If {@code fileName} cannot be opened for reading
     */
    @Builder
    private CsvRowWriter(
            @NonNull final String targetName,
            final String outputNullValue,
            @NonNull final List<ColumnHeader> headers,
            final Charset fileCharset) {
        this.targetName = targetName;
        final CsvWriterSettings writerSettings = new CsvWriterSettings();
        this.headers = new ArrayList<>(headers);
        writerSettings.setHeaders(this.headers.stream().map(ColumnHeader::toString).toArray(String[]::new));

        // encode NULL as the user requests, or `,,` if not specified
        writerSettings.setNullValue(Objects.requireNonNullElse(outputNullValue, ""));
        if (outputNullValue == null || outputNullValue.isBlank()) {
            // If NULL is being encoded as a blank, then use an empty string
            // encoding which can be distinguished if needed.
            writerSettings.setEmptyValue("\"\"");
        } else {
            // otherwise just write it out as the empty string (i.e., without quotes)
            // like any other quoted values without whitespace inside
            writerSettings.setEmptyValue("");
        }
        // Err on the side of safety w.r.t. quoting parsed content that contains any whitespace
        // by not trimming said content _and_ by adding quotes if any whitespace characters are seen
        // after parsing.
        writerSettings.trimValues(false);
        writerSettings.setQuotationTriggers(' ', '\t', '\n', '\r', '\f');

        try {
            writer = new CsvWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(targetName),
                            Objects.requireNonNullElse(fileCharset, StandardCharsets.UTF_8)),
                    writerSettings);
        } catch (FileNotFoundException e) {
            throw new C3rRuntimeException("Unable to write to output CSV file " + targetName + ".", e);
        }
        writer.writeHeaders();
    }

    /**
     * Write a record to the store.
     *
     * @param row Data to be written (as is)
     */
    public void writeRow(@NonNull final Row<CsvValue> row) {
        final String[] toWrite = new String[headers.size()];
        for (int i = 0; i < toWrite.length; i++) {
            toWrite[i] = row.getValue(headers.get(i)).toString();
        }
        writer.writeRow(toWrite);
    }

    /**
     * Close the connection to the file.
     */
    @Override
    public void close() {
        writer.close();
    }

    /**
     * Write all pending data to the file.
     */
    @Override
    public void flush() {
        writer.flush();
    }
}
