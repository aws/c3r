// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowWriter;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for Spark to write CSV files to disk.
 */
public abstract class SparkCsvWriter {

    /**
     * Writes the Dataset to the root path.
     *
     * @param dataset         The data to write to CSV
     * @param targetName      The target path to write to
     * @param outputNullValue What should represent {@code null} in the output
     */
    public static void writeOutput(@NonNull final Dataset<Row> dataset,
                                   @NonNull final String targetName,
                                   final String outputNullValue) {
        final Map<String, String> options = new HashMap<>();
        options.put("outputNullValue", outputNullValue);
        final String headers = String.join(",", dataset.columns());
        options.put("headers", headers);
        options.put("sessionUuid", dataset.sparkSession().sessionUUID());
        dataset.write().mode(SaveMode.Append).options(options).format(Csv.class.getCanonicalName())
                .save(targetName);
    }

    /**
     * Constructs a CsvRowWriter for writing CSV files.
     *
     * @param partitionId The partition being processed
     * @param properties  A map of configuration settings
     * @return a CsvRowWriter for writing CSV files
     * @throws C3rRuntimeException if a path is not contained in the configuration settings
     */
    public static CsvRowWriter initWriter(final int partitionId, final Map<String, String> properties) {
        if (!properties.containsKey("path")) {
            throw new C3rRuntimeException("A `path` must be provided with the provided when writing.");
        }
        final Path path = Path.of(properties.get("path"));
        FileUtil.initDirectoryIfNotExists(path.toString());
        // Generate random file name matching normal Spark patterns.
        final String sessionUuid = properties.get("sessionUuid");
        final String formattedPartitionId = String.format("%05d", partitionId);
        final String target = path.resolve("part-" + formattedPartitionId + "-" + sessionUuid + ".csv").toString();
        FileUtil.initFileIfNotExists(target);
        final String outputNullValue = properties.get("outputNullValue");
        final List<ColumnHeader> headers = Arrays.stream(properties.get("headers").split(","))
                .map(ColumnHeader::new)
                .collect(Collectors.toList());
        final Charset fileCharset = properties.get("fileCharset") == null ?
                StandardCharsets.UTF_8 : Charset.forName(properties.get("fileCharset"));
        return CsvRowWriter.builder().targetName(target)
                .outputNullValue(outputNullValue)
                .headers(headers)
                .fileCharset(fileCharset)
                .build();
    }
}
