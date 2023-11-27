// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.spark.config.SparkEncryptConfig;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.jdk.CollectionConverters;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spark orchestration of the C3R SDK for encryption.
 *
 * <p>
 * Note that there are a few differences between C3R's standard offerings and orchestrating with Spark.
 *
 * <p>
 * The most important difference is the change in trust boundaries. When using the C3R normally, files exist on the same machine running
 * C3R. C3R never writes any data to disk unencrypted unless it is meant to be unencrypted in the output. With Spark, as an input file is
 * read, Spark is partitioning that data in memory and/or on disk before C3R ever gets an opportunity to encrypt it. This means that
 * unless your Spark instance is configured to encrypt these files, cleartext forms of data that will eventually be encrypted may be
 * written to disk and/or distributed to Spark Workers before it is encrypted. Further, Spark Workers may exist on other machines or
 * networks. If a Spark job fails, there could be cleartext copies of the input file leftover across your Spark infrastructure. It is up
 * to you to understand if this is permissible for your threat model and to configure your Spark server according to your needs.
 *
 * <p>
 * Second, this Spark orchestration is not managing file permissions for the output file. C3R normally sets this file to be RW by the Owner
 * only. Files written by Spark will inherit the permissions of where they are written.
 *
 * <p>
 * Third, Spark partitions and distributes the partitioned data before C3R drops columns that will not be included in the output. When
 * using the C3R SDK or CLI, these columns are dropped during the data load step before they're ever written to disk. If these columns
 * should never leave the initial location, they should be removed from the data before it is handed to this Spark orchestration.
 *
 * <p>
 * Fourth, Spark may partition the data and thus the output files. You may need to take additional steps to merge the data if downstream
 * steps require it be one file. Note that when using S3 and Glue with AWS Clean Rooms, this should not be necessary.
 *
 * <p>
 * Finally, certain functionality like shuffling rows, dropping columns, finding max length of values in a column, and finding duplicate
 * values in a column are all revised in this orchestration to take advantage of Spark. These are normally handled by C3R's
 * {@link RowMarshaller}. All of these functions will behave the same as they do with C3R except shuffling rows.
 * Instead of sorting on Nonces created using Java's {@code SecureRandom}, Spark is using its own {@code rand()} function for the shuffle.
 */
public abstract class SparkMarshaller {

    /**
     * Spark orchestration of C3R encryption.
     *
     * <p>
     * Please note that only {@code String} data types are currently supported.
     *
     * @param inputData     input Dataset to process
     * @param encryptConfig Encryption config to use for processing
     * @return The encrypted Dataset
     */
    public static Dataset<Row> encrypt(final Dataset<Row> inputData, final SparkEncryptConfig encryptConfig) {
        final List<ColumnInsight> columnInsights = encryptConfig.getTableSchema().getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());

        Dataset<Row> dataset = filterSourceColumnsBySchema(inputData, columnInsights);

        updateMaxValuesPerColumn(dataset, columnInsights);

        validateDuplicates(encryptConfig.getSettings(), dataset, columnInsights);

        dataset = shuffleData(dataset);

        dataset = mapSourceToTargetColumns(dataset, columnInsights);

        populateColumnPositions(dataset, columnInsights);

        dataset = marshalData(dataset, encryptConfig, columnInsights);

        return dataset;
    }

    /**
     * Filter source columns not in the schema.
     *
     * <p>
     * This is normally handled by C3R's {@link RowMarshaller} by dropping columns that won't be in the output
     * during the data load.
     *
     * @param rawInputData   the Dataset to filter
     * @param columnInsights Insights for all the columns to be processed
     * @return A Dataset containing only source columns defined in the schema
     */
    static Dataset<Row> filterSourceColumnsBySchema(final Dataset<Row> rawInputData, final List<ColumnInsight> columnInsights) {
        final Set<ColumnHeader> schemaSourceColumns = columnInsights.stream()
                .map(ColumnSchema::getSourceHeader)
                .collect(Collectors.toSet());
        final Set<ColumnHeader> inputColumns = Arrays.stream(rawInputData.columns())
                .map(ColumnHeader::new)
                .collect(Collectors.toSet());
        inputColumns.removeAll(schemaSourceColumns);
        Dataset<Row> toReturn = rawInputData;
        for (ColumnHeader columnHeader : inputColumns) {
            toReturn = toReturn.drop(columnHeader.toString());
        }
        return toReturn;
    }

    /**
     * Updates the passed ColumnInsights with the max value length of their columns. These values are used during encryption whenever
     * {@link PadType#MAX} is configured for a sealed column.
     *
     * <p>
     * This is normally handled by C3R's {@link RowMarshaller} tracking the size of each value being read in
     * during the data load.
     *
     * @param columnInsights Insights for all the columns to be processed
     * @param rawInputData   The Dataset to run the queries against
     */
    static void updateMaxValuesPerColumn(final Dataset<Row> rawInputData,
                                         final List<ColumnInsight> columnInsights) {
        final Map<ColumnHeader, List<ColumnInsight>> sourceMappedColumnInsights = columnInsights.stream()
                .collect(Collectors.groupingBy(ColumnInsight::getSourceHeader));
        Arrays.stream(rawInputData.columns()).forEach(col -> {
            final Dataset<Row> columnData = rawInputData.select(functions.col(col)
                    .as("column"));
            columnData.createOrReplaceTempView("singleColumnData");
            final ColumnHeader columnHeader = new ColumnHeader(col);
            final Row longestValueRow = rawInputData.sparkSession()
                    .sql("SELECT max(bit_length(column))\n" +
                            "FROM singleColumnData\n" +
                            "ORDER BY bit_length('column') DESC\n" +
                            "LIMIT 1")
                    .first();
            final int maxBitLength = (longestValueRow.get(0) == null) ? 0 : longestValueRow.getInt(0);
            final int maxByteLength = maxBitLength / Byte.SIZE;
            for (ColumnInsight insight : sourceMappedColumnInsights.get(columnHeader)) {
                insight.setMaxValueLength(maxByteLength);
            }
        });
    }

    /**
     * Validates whether the input data meets the encryption settings for `allowDuplicates`.
     *
     * <p>
     * This is normally handled by C3R's {@link RowMarshaller} querying the temporary SQL table data is loaded
     * to.
     *
     * @param clientSettings The encryption settings to validate with
     * @param rawInputData   The Dataset to be validated
     * @param columnInsights Insights for all the columns to be processed
     * @throws C3rRuntimeException If input data is invalid
     */
    static void validateDuplicates(final ClientSettings clientSettings, final Dataset<Row> rawInputData,
                                   final List<ColumnInsight> columnInsights) {
        if (clientSettings.isAllowDuplicates()) {
            return;
        }
        // Check for duplicates when `allowDuplicates` is false
        final String[] fingerprintColumns = columnInsights.stream()
                .filter(columnSchema -> columnSchema.getType() == ColumnType.FINGERPRINT) // enforced on fingerprint columns only
                .map(ColumnSchema::getSourceHeader)
                .map(ColumnHeader::toString)
                .distinct()
                .toArray(String[]::new);

        // Check for duplicate non-null values
        for (String col : fingerprintColumns) {
            final Dataset<Row> filteredData = rawInputData.groupBy(col).count().filter("count > 1");
            if (!filteredData.isEmpty()) {
                throw new C3rRuntimeException("Duplicates were found in column `" + col + "`, but `allowDuplicates` is false.");
            }
        }
        // Check for duplicate null values when `preserveNulls` is false
        if (!clientSettings.isPreserveNulls()) {
            for (String col : fingerprintColumns) {
                final Column column = new Column(col);
                final Dataset<Row> filteredData = rawInputData.select(column)
                        .groupBy(column)
                        .count()
                        .filter(column.isNull())
                        .filter("count > 1");
                if (!filteredData.isEmpty()) {
                    throw new C3rRuntimeException("Duplicates NULLs were found in column `" + col + "`, but `allowDuplicates` and " +
                            "`preserveNulls` are false.");
                }
            }
        }
    }

    /**
     * Map the source columns to their respective target columns.
     *
     * <p>
     * This is normally handled by C3R's {@link RowMarshaller} by writing input columns of data to the intended
     * target columns during the data load.
     *
     * @param rawInputData   the Dataset to map
     * @param columnInsights Insights for all the columns to be processed
     * @return A Dataset containing each target column
     */
    static Dataset<Row> mapSourceToTargetColumns(final Dataset<Row> rawInputData, final List<ColumnInsight> columnInsights) {
        final List<Column> targetColumns = new ArrayList<>();
        columnInsights.forEach(target -> targetColumns.add(functions.col(target.getSourceHeader().toString())
                .as(target.getTargetHeader().toString())));
        return rawInputData.select(CollectionConverters.IteratorHasAsScala(targetColumns.iterator()).asScala().toSeq());
    }

    /**
     * Encrypt source data. Requires that column positions have been populated in ColumnInsights.
     *
     * @param rawInputData   The source data to be encrypted
     * @param encryptConfig  The encryption configuration
     * @param columnInsights Insights for all the columns to be processed
     * @return The encrypted data
     * @throws C3rRuntimeException If the input data cannot be marshalled.
     */
    static Dataset<Row> marshalData(final Dataset<Row> rawInputData, final SparkEncryptConfig encryptConfig,
                                    final List<ColumnInsight> columnInsights) {
        // Copy out values that need to be serialized
        final ClientSettings settings = encryptConfig.getSettings();
        final String salt = encryptConfig.getSalt();
        final String base64EncodedKey = Base64.getEncoder().encodeToString(encryptConfig.getSecretKey().getEncoded());

        final ExpressionEncoder<Row> rowEncoder = ExpressionEncoder.apply(rawInputData.schema());
        final StructField[] fields = rawInputData.schema().fields();
        try {
            return rawInputData.map((MapFunction<Row, Row>) row -> {
                // Grab a nonce for the row
                final Nonce nonce = Nonce.nextNonce();
                // Build a list of transformers for each row, limiting state to keys/salts/settings POJOs
                final Map<ColumnType, Transformer> transformers = Transformer.initTransformers(
                        KeyUtil.sharedSecretKeyFromString(base64EncodedKey),
                        salt,
                        settings,
                        false); // Not relevant to encryption
                // For each column in the row, transform the data
                return Row.fromSeq(
                        CollectionConverters.IteratorHasAsScala(columnInsights.stream().map(column -> {
                            if (column.getType() == ColumnType.CLEARTEXT) {
                                return row.get(column.getSourceColumnPosition());
                            }
                            if (fields[column.getSourceColumnPosition()].dataType() != DataTypes.StringType) {
                                throw new C3rRuntimeException("Encrypting non-String values is not supported. Non-String column marked" +
                                        " for encryption: `" + column.getTargetHeader() + "`");
                            }
                            final Transformer transformer = transformers.get(column.getType());
                            final String data = row.getString(column.getSourceColumnPosition());
                            final byte[] dataBytes = data == null ? null : data.getBytes(StandardCharsets.UTF_8);
                            final EncryptionContext encryptionContext = new EncryptionContext(column, nonce, ClientDataType.STRING);
                            final byte[] marshalledBytes = transformer.marshal(dataBytes, encryptionContext);
                            return (marshalledBytes == null ? null : new String(marshalledBytes, StandardCharsets.UTF_8));
                        }).iterator()).asScala().toSeq());
            }, rowEncoder);
        } catch (C3rRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new C3rRuntimeException("Unknown exception when encrypting data.", e);
        }
    }

    /**
     * Find the positions for each column. Requires that source columns have already been mapped to targets.
     *
     * <p>
     * Note that this method repurposes the {@link ColumnInsight#getSourceColumnPosition()} method to track the position of a target
     * column in the source data after the source columns have been mapped to target column names.
     *
     * @param rawInputData   The source data to map the columns with
     * @param columnInsights Insights for all the columns to be processed
     */
    static void populateColumnPositions(final Dataset<Row> rawInputData, final List<ColumnInsight> columnInsights) {
        // Gather the positions of all the columns
        final String[] columns = rawInputData.columns();
        final Map<ColumnHeader, Integer> columnPositions = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnPositions.put(new ColumnHeader(columns[i]), i);
        }
        for (ColumnInsight column : columnInsights) {
            final int position = columnPositions.get(column.getTargetHeader());
            column.setSourceColumnPosition(position);
        }
    }

    /**
     * Shuffles the input data to hide ordering.
     *
     * <p>
     * This is normally handled by C3R's {@link RowMarshaller} by appending the Nonces used for each row to the
     * data on load and then sorting on those nonces before writing out the data. Instead of sorting on Nonces created using Java's
     * {@code SecureRandom}, Spark is using its own {@code rand()} function for the shuffle.
     *
     * @param rawInputData The Dataset to shuffle
     * @return The shuffled Dataset
     */
    static Dataset<Row> shuffleData(final Dataset<Row> rawInputData) {
        return rawInputData.orderBy(functions.rand());
    }
}
