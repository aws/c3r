// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Specific implementation for reading CSV files in preparation for data marshalling.
 */
public final class CsvRowReader extends RowReader<CsvValue> {
    /**
     * Maximum number of columns allowed.
     *
     * <p>
     * This is defined at the implementation layer and not the RowReader interface in order to allow tuning among different formats.
     */
    static final int MAX_COLUMN_COUNT = 1600;

    /**
     * Name of the CSV file for error reporting purposes.
     */
    @Getter
    final String sourceName;

    /**
     * Source CSV content is read from.
     */
    private final Reader reader;

    /**
     * Charset in use for I/O.
     */
    private final Charset fileCharset;

    /**
     * CSV parser configured to user settings.
     */
    private final CsvParser parser;

    /**
     * Headers for columns in the CSV file.
     */
    @Getter
    private final List<ColumnHeader> headers;

    /**
     * Whether the reader has been closed.
     */
    private boolean closed = false;

    /**
     * The next row to be returned.
     */
    private Row<CsvValue> nextRow;

    /**
     * Creates a record reader for a CSV file.
     *
     * <p>
     * For customizing NULL treatment in input data, the `inputNullValue` parameter has the following semantics:
     * <ul>
     *   <li> If `inputNullValue` is `null` then any blank (e.g., `,,`, `, ,`, etc) entry will be interpreted as NULL
     * and empty quotes (`,"",`) will be interpreted as NULL, </li>
     * <li> else if `inputNullValue.isBlank()` then any blank (e.g., `,,`, `, ,`, etc) entry will be interpreted
     * as NULL but no quoted values will be interpreted as NULL. </li>
     * <li>else if `inputNullValue` is `"\"\""` then _only_ `"\"\""` will be interpreted as NULL</li>
     * <li>else non-blank values matching `inputNullValue` after being parsed will be considered NULL.</li>
     * </ul>
     *
     * @param sourceName      File name to be read as a CSV file path
     * @param inputNullValue  What should be interpreted as {@code null} in the input
     * @param externalHeaders Strings to use as column header names if the file itself does not contain a header row
     * @param fileCharset     Character set of the file. Defaults to {@code UTF_8} if {@code null}
     * @throws C3rIllegalArgumentException If number of columns in file doesn't match number of columns in PositionalTableSchema or a parse
     *                                     error occurs
     * @throws C3rRuntimeException         If the file can't be read
     */
    @Builder
    private CsvRowReader(@NonNull final String sourceName,
                         final String inputNullValue,
                         final List<ColumnHeader> externalHeaders,
                         final Charset fileCharset) {
        this.sourceName = sourceName;
        this.fileCharset = fileCharset == null ? StandardCharsets.UTF_8 : fileCharset;
        try {
            this.reader = new InputStreamReader(new FileInputStream(sourceName), this.fileCharset.newDecoder());
        } catch (FileNotFoundException e) {
            throw new C3rRuntimeException("Unable to read source file " + sourceName + ".", e);
        }

        // Gather all the information we need for CSV parsing
        final ParserConfiguration state = generateParserSettings(externalHeaders, inputNullValue);

        // creates a CSV parser
        parser = new CsvParser(state.csvParserSettings);

        // Handle headers and row count validations depending on schema type
        if (externalHeaders == null) {
            this.headers = setupForHeaderFileParsing(state);
        } else {
            this.headers = setupForNoHeaderFileParsing(state, externalHeaders);
        }
        refreshNextRow();
    }

    /**
     * Open a reader for the given file and Charset such that explicit errors are thrown if
     * the encountered file contents is not in the expected Charset.
     *
     * @param sourceFile  File to open
     * @param fileCharset Charset to interpret file as, throwing an error if invalid data is encountered.
     * @return A reader for the file
     * @throws C3rIllegalArgumentException If the file is not found
     */
    private static InputStreamReader openReaderForFile(@NonNull final String sourceFile,
                                                       @NonNull final Charset fileCharset) {
        try {
            // `fileCharset.newDecoder()` ensures an error is thrown if the content of the file is
            // not compatible with the specified Charset
            return new InputStreamReader(new FileInputStream(sourceFile), fileCharset.newDecoder());
        } catch (FileNotFoundException e) {
            throw new C3rIllegalArgumentException("Unable to locate file " + sourceFile + ".", e);
        }
    }

    /**
     * Parse the first line in a CSV file to extract the column count.
     *
     * @param csvFileName CSV file to count columns in
     * @param fileCharset Character encoding in file (defaults to {@link CsvRowReader} default encoding if {@code null})
     * @return The column count for the given file
     * @throws C3rRuntimeException If an I/O error occurs reading the file
     */
    public static int getCsvColumnCount(@NonNull final String csvFileName, final Charset fileCharset) {
        final Charset charset = fileCharset == null ? StandardCharsets.UTF_8 : fileCharset;
        final ParserConfiguration state = generateParserSettings(null, null);
        try (var reader = openReaderForFile(csvFileName, charset)) {
            state.csvParserSettings.setHeaderExtractionEnabled(false);
            final CsvParser parser = new CsvParser(state.csvParserSettings);
            beginParsing(parser, reader, charset);
            final String[] firstLine = executeTextParsing(parser::parseNext);
            parser.stopParsing();
            if (firstLine == null) {
                throw new C3rRuntimeException("Could not read a CSV line from the file " + csvFileName);
            }
            return firstLine.length;
        } catch (TextParsingException e) {
            throw new C3rRuntimeException("Could not get column count: an error occurred while parsing " + csvFileName, e);
        } catch (IOException e) {
            throw new C3rRuntimeException("Could not get column count: an I/O error occurred while reading " + csvFileName, e);
        }
    }

    /**
     * Takes in information on headers and null values to create settings for parsing.
     *
     * @param headers        List of unspecified header names for if there are any (positional schemas only use this)
     * @param inputNullValue Value to be used for a custom null
     * @return All configuration information needed throughout parsing
     */
    private static ParserConfiguration generateParserSettings(final List<ColumnHeader> headers, final String inputNullValue) {
        final ParserConfiguration state = new ParserConfiguration();
        state.csvParserSettings = new CsvParserSettings();

        // configure reader settings
        state.csvParserSettings.setLineSeparatorDetectionEnabled(true);

        // `setNullValue` sets the value used when no characters appear in an entry (after trimming)
        // `setEmptyValue` sets the value used when no characters appear within a quoted entry (`,"",`)
        state.toNullConversionRequired = false;
        if (inputNullValue == null) {
            state.csvParserSettings.setNullValue(null);
            state.csvParserSettings.setEmptyValue(null);
        } else if (inputNullValue.isBlank()) {
            state.csvParserSettings.setNullValue(null);
            state.csvParserSettings.setEmptyValue("");
        } else if (inputNullValue.trim().equals("\"\"")) {
            state.csvParserSettings.setNullValue("");
            state.csvParserSettings.setEmptyValue(null);
        } else {
            state.csvParserSettings.setNullValue("");
            state.csvParserSettings.setEmptyValue("");
            state.toNullConversionRequired = true;
        }

        // Set maximum number of supported columns
        state.csvParserSettings.setMaxColumns(MAX_COLUMN_COUNT);

        // Disable the check for max chars per column. This is enforced by the Transformers as they're processed and may unnecessarily
        // restrict user data being used for column types like `fingerprint`.
        state.csvParserSettings.setMaxCharsPerColumn(-1);

        // Check if this is a positional file and turn off header extraction if it is
        state.csvParserSettings.setHeaderExtractionEnabled(headers == null);

        // Save custom null value for when we need it for substitution
        state.nullValue = inputNullValue;

        // Save the number of columns expected in a positional file
        state.numberOfColumns = (headers == null) ? null : headers.size();

        return state;
    }

    /**
     * Starts iteration style parsing of the CSV file.
     *
     * @param parser      CSV data parser
     * @param reader      Source to read CSV data from
     * @param fileCharset Charset for the contents the reader is reading
     * @throws C3rRuntimeException If the file can't be parsed
     */
    private static void beginParsing(@NonNull final CsvParser parser,
                                     @NonNull final Reader reader,
                                     @NonNull final Charset fileCharset) {
        try {
            parser.beginParsing(reader);
        } catch (TextParsingException e) {
            throw new C3rRuntimeException("Couldn't begin parsing of input file. This is most likely due to the file not being correctly" +
                    " formatted as " + fileCharset + ". Please review the stack trace for more details.", e);
        }
    }

    /**
     * Gets the parser set up to read a CSV file with header rows.
     * - Gets headers from file to use for mapping values
     * - Sets up null value conversion if needed
     *
     * @param state Parser configuration
     * @return Name of all columns
     * @throws C3rRuntimeException If the file can't be read
     */
    private List<ColumnHeader> setupForHeaderFileParsing(final ParserConfiguration state) {
        // This file has headers, read them from file
        beginParsing(parser, reader, fileCharset);
        final String[] headersPreCheck = executeTextParsing(() -> parser.getRecordMetadata().headers());
        if (headersPreCheck == null) {
            throw new C3rRuntimeException(String.format("Unable to read headers from %s", sourceName));
        }
        // Set null value if needed
        if (state.toNullConversionRequired) {
            executeTextParsing(() -> parser.getRecordMetadata().convertFields(Conversions.toNull(state.nullValue)).set(headersPreCheck));
        }
        // Return headers for mapping
        return Arrays.stream(headersPreCheck).map(ColumnHeader::new).collect(Collectors.toList());
    }

    /**
     * Sets up the parser for reading a CSV file without headers.
     *
     * @param state   Parser configuration
     * @param headers Names for unspecified columns
     * @return Name of all columns
     * @throws C3rRuntimeException If unable to access CSV file or a parse error occurs
     */
    private List<ColumnHeader> setupForNoHeaderFileParsing(final ParserConfiguration state, final List<ColumnHeader> headers) {
        // Check to make sure CSV column count matches what we think we have
        // This uses a unique reader for this to avoid extraneous error handling by using file markers and rewinding the file
        try (Reader columnCounter = new InputStreamReader(new FileInputStream(sourceName), ((InputStreamReader) reader).getEncoding())) {
            beginParsing(parser, columnCounter, fileCharset);
            setNullValueState(state);
            final Record record = executeTextParsing(parser::parseNextRecord);
            if (record.getValues().length != state.numberOfColumns) {
                throw new C3rRuntimeException("Positional table schemas must match the same number of columns as the data. " +
                        "Expected: " + state.numberOfColumns + ", found: " + record.getValues().length + ".");
            }
            parser.stopParsing();
        } catch (IOException e) {
            throw new C3rRuntimeException("Unable to access source file " + sourceName + ".", e);
        }

        // Change to primary reader and set null value if needed
        beginParsing(parser, reader, fileCharset);
        setNullValueState(state);

        return Collections.unmodifiableList(headers);
    }

    /**
     * Looks at the parser configuration and sets the null value representation for each column.
     *
     * @param state Parser configuration
     */
    private void setNullValueState(final ParserConfiguration state) {
        // Set null value if needed
        if (state.toNullConversionRequired) {
            final ArrayList<Integer> indices = new ArrayList<>();
            for (int i = 0; i < state.numberOfColumns; i++) {
                indices.add(i);
            }
            executeTextParsing(() -> parser.getRecordMetadata().convertIndexes(Conversions.toNull(state.nullValue)).set(indices));
        }
    }

    /**
     * Stage the next CSV row.
     *
     * @throws C3rRuntimeException If there's a mismatch between the expected column count and the read column count
     */
    protected void refreshNextRow() {
        if (closed) {
            nextRow = null;
            return;
        }
        final Record record = executeTextParsing(parser::parseNextRecord);
        if (record == null) {
            nextRow = null;
            return;
        }
        if (record.getValues().length != headers.size()) {
            throw new C3rRuntimeException("Column count mismatch at row " + this.getReadRowCount() + " of input file. Expected "
                    + headers.size() + " columns, but found " + record.getValues().length + ".");
        }
        nextRow = new CsvRow();
        for (int i = 0; i < headers.size(); i++) {
            nextRow.putValue(headers.get(i), new CsvValue(record.getString(i)));
        }
    }

    /**
     * Look at the next CSV row.
     *
     * @return Parsed and normalized CSV values
     */
    protected Row<CsvValue> peekNextRow() {
        return nextRow;
    }

    /**
     * Close the resources reading the CSV file.
     *
     * @throws C3rRuntimeException If there's an error closing the file connection
     */
    @Override
    public void close() {
        if (!closed) {
            parser.stopParsing();
            try {
                reader.close();
            } catch (IOException e) {
                throw new C3rRuntimeException("Error closing connection to CSV file.", e);
            }
            closed = true;
        }
    }

    /**
     * Adds information when the CSV parser encounters an error to give the user more context.
     *
     * @param executable Call to CsvParser
     * @param <T>        The type that is returned by the supplier if no text parsing errors occur
     * @return A parsed value
     * @throws C3rRuntimeException If the data can't be parsed
     */
    private static <T> T executeTextParsing(final Supplier<T> executable) {
        try {
            return executable.get();
        } catch (TextParsingException e) {
            if (e.getColumnIndex() > MAX_COLUMN_COUNT - 1) {
                throw new C3rRuntimeException("Couldn't parse row " + (e.getLineIndex() + 1) + " at column " + (e.getColumnIndex() + 1)
                        + " of input file. Please verify that column count does not exceed " + MAX_COLUMN_COUNT, e);
            } else {
                throw new C3rRuntimeException("An unknown error occurred parsing row " + (e.getLineIndex() + 1) + " at column " +
                        (e.getColumnIndex() + 1) + " of input file. Please review the stack trace for more details.", e);
            }
        }
    }

    /**
     * Simple class used to hold the CsvParserSettings from the parsing library plus three other settings we need to properly parse.
     */
    private static class ParserConfiguration {
        /**
         * Univocity parser settings.
         */
        private CsvParserSettings csvParserSettings;

        /**
         * Are we using a custom null value.
         */
        private boolean toNullConversionRequired;

        /**
         * Custom null value, if any.
         */
        private String nullValue;

        /**
         * How many columns are in the table.
         */
        private Integer numberOfColumns;
    }
}
