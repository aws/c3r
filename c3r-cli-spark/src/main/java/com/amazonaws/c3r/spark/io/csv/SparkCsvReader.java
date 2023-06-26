// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.spark.config.SparkConfig;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for Spark to read CSV files from disk.
 */
public abstract class SparkCsvReader {

    /**
     * Reads the input file for processing, normalizing headers.
     *
     * @param sparkSession            The spark session to read with
     * @param source                  Location of input data
     * @param inputNullValue          What should be interpreted as {@code null} in the input
     * @param externalHeaders         Strings to use as column header names if the file itself does not contain a header row
     * @return The source data to be processed
     */
    public static Dataset<Row> readInput(@NonNull final SparkSession sparkSession,
                                         @NonNull final String source,
                                         final String inputNullValue,
                                         final List<ColumnHeader> externalHeaders) {
        return readInput(sparkSession, source, inputNullValue, externalHeaders, false);
    }

    /**
     * Reads the input file for processing.
     *
     * @param sparkSession            The spark session to read with
     * @param source                  Location of input data
     * @param inputNullValue          What should be interpreted as {@code null} in the input
     * @param externalHeaders         Strings to use as column header names if the file itself does not contain a header row
     * @param skipHeaderNormalization Whether to skip the normalization of read in headers
     * @return The source data to be processed
     */
    public static Dataset<Row> readInput(@NonNull final SparkSession sparkSession,
                                         @NonNull final String source,
                                         final String inputNullValue,
                                         final List<ColumnHeader> externalHeaders,
                                         final boolean skipHeaderNormalization) {
        final Map<String, String> options = new HashMap<>();
        options.put("inputNullValue", inputNullValue);
        options.put(SparkConfig.PROPERTY_KEY_SKIP_HEADER_NORMALIZATION, Boolean.toString(skipHeaderNormalization));
        if (externalHeaders != null && !externalHeaders.isEmpty()) {
            options.put("headers", externalHeaders.stream().map(ColumnHeader::toString).collect(Collectors.joining(",")));
        }
        return readFiles(sparkSession, source, options);
    }

    /**
     * Reads the input for processing. If it is a directory, recurse over each file in the directory.
     *
     * @param sparkSession The spark session to read with
     * @param source       Location of input data
     * @param options      Configuration options
     * @return The source data to be processed
     * @throws C3rRuntimeException If the source cannot be read
     */
    private static Dataset<Row> readFiles(@NonNull final SparkSession sparkSession,
                                          @NonNull final String source,
                                          final Map<String, String> options) {
        final File sourceFile = Path.of(source).toFile();
        if (sourceFile.isFile()) {
            return readFile(sparkSession, source, options);
        }
        final File[] files = sourceFile.listFiles();
        if (files == null) {
            throw new C3rRuntimeException("Source could not be read at path " + sourceFile + ".");
        }
        Dataset<Row> dataset = null;
        Set<String> columns = null;
        for (File file : files) {
            if (file.isDirectory()) {
                continue; // Skip directories. Recursion not currently supported.
            }
            if (dataset == null) {
                dataset = readFile(sparkSession, file.getAbsolutePath(), options);
                columns = Set.of(dataset.columns());
            } else {
                final Dataset<Row> nextDataset = readFile(sparkSession, file.getAbsolutePath(), options);
                final Set<String> nextDatasetColumns = Set.of(nextDataset.columns());
                if (columns.size() != nextDatasetColumns.size() || !columns.containsAll(nextDatasetColumns)) {
                    // unionAll will merge data based on column position without further enforcement of schemas.
                    throw new C3rRuntimeException("Found mismatched columns between "
                            + files[0].getAbsolutePath() + " and " + file.getAbsolutePath() + ".");
                }
                // We must use unionAll and not union because union filters on distinct rows.
                dataset = dataset.unionAll(nextDataset);
            }
        }
        return dataset;
    }

    /**
     * Reads the input file for processing.
     *
     * @param sparkSession The spark session to read with
     * @param sourceFile   Location of input data
     * @param options      Configuration options
     * @return The source data to be processed
     */
    private static Dataset<Row> readFile(@NonNull final SparkSession sparkSession,
                                         @NonNull final String sourceFile,
                                         final Map<String, String> options) {
        return sparkSession.read()
                .options(options)
                .format(Csv.class.getCanonicalName())
                .load(sourceFile);
    }

    /**
     * Constructs a CsvRowReader for parsing CSV files.
     *
     * @param properties A map of configuration settings
     * @return a CsvRowReader for parsing CSV files
     * @throws C3rRuntimeException if a path is not contained in the configuration settings
     */
    public static CsvRowReader initReader(final Map<String, String> properties) {
        if (!properties.containsKey("path")) {
            throw new C3rRuntimeException("A `path` must be provided when reading.");
        }
        final boolean skipHeaderNormalization =
                properties.getOrDefault(SparkConfig.PROPERTY_KEY_SKIP_HEADER_NORMALIZATION, "false")
                        .equalsIgnoreCase("true");
        final String source = properties.get("path");
        final String inputNullValue = properties.get("inputNullValue");
        final List<ColumnHeader> externalHeaders = properties.get("headers") == null ?
                null : Arrays.stream(properties.get("headers").split(","))
                .map(ColumnHeader::new)
                .collect(Collectors.toList());
        final Charset fileCharset = properties.get("fileCharset") == null ?
                StandardCharsets.UTF_8 : Charset.forName(properties.get("fileCharset"));
        return CsvRowReader.builder().sourceName(source)
                .inputNullValue(inputNullValue)
                .externalHeaders(externalHeaders)
                .fileCharset(fileCharset)
                .skipHeaderNormalization(skipHeaderNormalization)
                .build();
    }
}
