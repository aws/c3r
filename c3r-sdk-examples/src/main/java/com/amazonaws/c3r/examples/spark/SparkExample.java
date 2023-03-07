// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples.spark;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Nonce;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import scala.jdk.CollectionConverters;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Example code for running Spark.
 *
 * <p>
 * Note that there are a few differences between C3R's pre-packaged offerings and orchestrating with Spark.
 *
 * <p>
 * The most important difference is the change in trust boundaries. When using the C3R normally, files exist on the same machine running
 * C3R. C3R never writes any data to disk unencrypted unless it is meant to be unencrypted in the output. With Spark, as an input file is
 * read, Spark is partitioning that data in memory and/or on disk before C3R ever gets an opportunity to encrypt it. This means that
 * cleartext forms of data that will eventually be encrypted may be written to disk and/or distributed to Spark Workers before it is
 * encrypted. Further, Spark Workers may exist on other machines or networks. If a Spark job fails, there could be
 * cleartext copies of the input file leftover across your Spark infrastructure. It is up to you to understand if this is permissible
 * for your threat model and to configure your Spark server according to your needs.
 *
 * <p>
 * Second, this Spark example is not managing file permissions for the output file. C3R normally sets this file to be RW by the Owner
 * only. Files written by Spark will inherit the permissions of where they are written.
 *
 * <p>
 * Third, Spark partitions and distributes the cleartext data before C3R drops columns that will not be included in the output. When
 * using the C3R SDK or CLI, these columns are dropped during the data load step before they're ever written to disk. If these columns
 * should never leave the initial location, they should be removed from the data before it is handed to this Spark example.
 *
 * <p>
 * Fourth, Spark may partition the data and thus the output files. You may need to take additional steps to merge the data if downstream
 * steps require it be one file. Note that when using S3 and Glue with AWS Clean Rooms, this should not be necessary.
 *
 * <p>
 * Finally, certain functionality like shuffling rows, dropping columns, finding max length of values in a column, and finding duplicate
 * values in a column are all revised in this example to take advantage of Spark. These are normally handled by C3R's
 * {@link com.amazonaws.c3r.action.RowMarshaller}. All of these functions will behave the same as they do with C3R except shuffling rows.
 * Instead of sorting on Nonces created using Java's {@code SecureRandom}, Spark is using its own {@code rand()} function for the shuffle.
 */
public final class SparkExample {
    /**
     * An example 32-byte key used for testing.
     */
    private static final String EXAMPLE_SHARED_SECRET_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFrEXAMPLESECRET=";

    /**
     * Example collaboration ID, i.e., the value used by all participating parties as a salt for encryption.
     */
    private static final String EXAMPLE_SALT = "00000000-1111-2222-3333-444444444444";

    /**
     * Insights for all the target columns that will be written.
     */
    private static Collection<ColumnInsight> columnInsights;

    /** Hidden utility constructor. */
    private SparkExample() {
    }

    /**
     * Create a Spark session for running the encrypt/decrypt methods.
     *
     * <p>
     * This method will by default create a local Spark Driver. Modify the URL of the Spark Driver within this function to run
     * this example on another Spark Driver.
     *
     * @return A spark session
     */
    private static SparkSession initSparkSession() {
        // CHECKSTYLE:OFF
        final SparkConf conf = new SparkConf()
                .setAppName("C3RSparkDemo")
                // Update this to point to your own Spark Driver if not running this locally.
                .setMaster("local[*]");
        // CHECKSTYLE:ON

        return SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
    }

    /**
     * Sample of Spark orchestrating the C3R SDK for encryption.
     *
     * <p>
     * This function is currently setup to only process CSV files. It can be modified to instead take a {@code Dataset<Row>}. There is no
     * functionality specific to a CSV after the initial data load.
     *
     * <p>
     * Please note that only {@code String} data types are currently supported.
     *
     * @param source input file
     * @param target output file
     * @param schema schema file
     */
    public static void encrypt(final String source, final String target, final TableSchema schema) {
        final SparkSession spark = initSparkSession();

        final ClientSettings clientSettings = ClientSettings.lowAssuranceMode();

        columnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());

        Dataset<Row> rawInputData = readInput(source, spark);

        rawInputData = filterSourceColumnsBySchema(rawInputData);

        updateMaxValuesPerColumn(spark, rawInputData);

        validateDuplicates(clientSettings, rawInputData);

        rawInputData = shuffleData(rawInputData);

        rawInputData = mapSourceToTargetColumns(rawInputData);

        populateColumnPositions(rawInputData);

        rawInputData = marshalData(rawInputData);

        rawInputData.write().mode(SaveMode.Append).option("header", true).csv(target);

        closeSparkSession(spark);
    }

    /**
     * Sample of Spark orchestrating the C3R SDK for decryption.
     *
     * <p>
     * This function is currently setup to only process CSV files. It can be modified to instead take a {@code Dataset<Row>}. There is no
     * functionality specific to a CSV after the initial data load.
     *
     * <p>
     * Please note that only {@code String} data types are currently supported.
     *
     * @param source input file
     * @param target output file
     */
    public static void decrypt(final String source, final String target) {
        final SparkSession spark = initSparkSession();

        Dataset<Row> rawInputData = readInput(source, spark);

        rawInputData = unmarshalData(rawInputData);

        rawInputData.write().mode(SaveMode.Append).option("header", true).csv(target);

        closeSparkSession(spark);
    }

    /**
     * Reads the input file for processing.
     *
     * <p>
     * NOTE: Empty values in CSVs are treated as null by default when Spark parses them. To configure nulls with
     * Spark, see the <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html">Spark documentation on CSVs</a>.
     *
     * @param source input file
     * @param spark  the SparkSession to read with
     * @return The source data to be processed
     */
    private static Dataset<Row> readInput(final String source, final SparkSession spark) {
        return spark.read()
                .option("header", "true") // Filter out the header row
                .option("inferSchema", "false") // Treat all fields as Strings
                .option("nullValue", null)
                .option("emptyValue", null)
                .csv(source);
    }

    /**
     * Filter source columns not in the schema.
     *
     * <p>
     * This is normally handled by C3R's {@link com.amazonaws.c3r.action.RowMarshaller} by dropping columns that won't be in the output
     * during the data load.
     *
     * @param rawInputData the Dataset to filter
     * @return A Dataset containing only source columns defined in the schema
     */
    static Dataset<Row> filterSourceColumnsBySchema(final Dataset<Row> rawInputData) {
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
     * Updates {@link #columnInsights} with the max value length of their columns. These values are used during encryption whenever
     * {@link com.amazonaws.c3r.config.PadType#MAX} is configured for a sealed column.
     *
     * <p>
     * This is normally handled by C3R's {@link com.amazonaws.c3r.action.RowMarshaller} tracking the size of each value being read in
     * during the data load.
     *
     * @param spark        The SparkSession to run the queries in
     * @param rawInputData The Dataset to run the queries against
     */
    static void updateMaxValuesPerColumn(final SparkSession spark, final Dataset<Row> rawInputData) {
        rawInputData.createOrReplaceTempView("rawData");
        final Map<ColumnHeader, List<ColumnInsight>> sourceMappedColumnInsights = columnInsights.stream()
                .collect(Collectors.groupingBy(ColumnInsight::getSourceHeader));

        Arrays.stream(rawInputData.columns()).forEach(col -> {
            final int maxValue = spark.sql("SELECT max(length(" + col + ")) FROM rawData").first().getInt(0);
            final ColumnHeader columnHeader = new ColumnHeader(col);
            for (ColumnInsight insight : sourceMappedColumnInsights.get(columnHeader)) {
                insight.setMaxValueLength(maxValue);
            }
        });
    }

    /**
     * Validates whether the input data meets the encryption settings for `allowDuplicates`.
     *
     * <p>
     * This is normally handled by C3R's {@link com.amazonaws.c3r.action.RowMarshaller} querying the temporary SQL table data is loaded
     * to.
     *
     * @param clientSettings The encryption settings to validate with
     * @param rawInputData   The Dataset to be validated
     * @throws C3rRuntimeException If input data is invalid
     */
    static void validateDuplicates(final ClientSettings clientSettings, final Dataset<Row> rawInputData) {
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
     * This is normally handled by C3R's {@link com.amazonaws.c3r.action.RowMarshaller} by writing input columns of data to the intended
     * target columns during the data load.
     *
     * @param rawInputData the Dataset to map
     * @return A Dataset containing each target column
     */
    static Dataset<Row> mapSourceToTargetColumns(final Dataset<Row> rawInputData) {
        final List<Column> targetColumns = new ArrayList<>();
        columnInsights.forEach(target -> targetColumns.add(functions.col(target.getSourceHeader().toString())
                .as(target.getTargetHeader().toString())));
        return rawInputData.select(CollectionConverters.IteratorHasAsScala(targetColumns.iterator()).asScala().toSeq());
    }

    /**
     * Encrypt source data.
     *
     * @param rawInputData The source data to be encrypted
     * @return The encrypted data
     */
    static Dataset<Row> marshalData(final Dataset<Row> rawInputData) {
        final ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(rawInputData.schema());
        return rawInputData.map((MapFunction<Row, Row>) row -> {
            // Grab a nonce for the row
            final Nonce nonce = Nonce.nextNonce();
            // Build a list of transformers for each row, limiting state to keys/salts/settings POJOs
            final Map<ColumnType, Transformer> transformers = Transformer.initTransformers(
                    KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY),
                    EXAMPLE_SALT,
                    ClientSettings.lowAssuranceMode(),
                    false); // Defaulting to false.
            // For each column in the row, transform the data
            return Row.fromSeq(
                    CollectionConverters.IteratorHasAsScala(columnInsights.stream().map(column -> {
                        if (column.getType() == ColumnType.CLEARTEXT) {
                            return row.get(column.getSourceColumnPosition());
                        }
                        final Transformer transformer = transformers.get(column.getType());
                        final String data = row.getString(column.getSourceColumnPosition());
                        final byte[] dataBytes = data == null ? null : data.getBytes(StandardCharsets.UTF_8);
                        final EncryptionContext encryptionContext = new EncryptionContext(column, nonce, ClientDataType.STRING);
                        final byte[] marshalledBytes = transformer.marshal(dataBytes, encryptionContext);
                        return (marshalledBytes == null ? null : new String(marshalledBytes, StandardCharsets.UTF_8));
                    }).iterator()).asScala().toSeq());
        }, rowEncoder);
    }

    /**
     * Decrypt source data.
     *
     * @param rawInputData The source data to be decrypted
     * @return The cleartext data
     */
    static Dataset<Row> unmarshalData(final Dataset<Row> rawInputData) {
        final ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(rawInputData.schema());
        return rawInputData.map((MapFunction<Row, Row>) row -> {
            // Build a list of transformers for each row, limiting state to keys/salts/settings POJOs
            final Map<ColumnType, Transformer> transformers = Transformer.initTransformers(
                    KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY),
                    EXAMPLE_SALT,
                    ClientSettings.lowAssuranceMode(),
                    false); // Defaulting to false.
            // For each column in the row, transform the data
            final List<Object> unmarshalledValues = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                final String data = row.getString(i);
                final byte[] dataBytes = data == null ? null : data.getBytes(StandardCharsets.UTF_8);
                Transformer transformer = transformers.get(ColumnType.CLEARTEXT); // Default to pass through
                if (Transformer.hasDescriptor(transformers.get(ColumnType.SEALED), dataBytes)) {
                    transformer = transformers.get(ColumnType.SEALED);
                } else if (Transformer.hasDescriptor(transformers.get(ColumnType.FINGERPRINT), dataBytes)) {
                    transformer = transformers.get(ColumnType.FINGERPRINT);
                }
                final byte[] unmarshalledBytes = transformer.unmarshal(dataBytes);
                unmarshalledValues.add(unmarshalledBytes == null ? null : new String(unmarshalledBytes, StandardCharsets.UTF_8));
            }

            return Row.fromSeq(
                    CollectionConverters.IteratorHasAsScala(unmarshalledValues.iterator()).asScala().toSeq());
        }, rowEncoder);
    }

    /**
     * Find the positions for each column.
     *
     * @param rawInputData The source data to map the columns with
     */
    static void populateColumnPositions(final Dataset<Row> rawInputData) {
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
     * This is normally handled by C3R's {@link com.amazonaws.c3r.action.RowMarshaller} by appending the Nonces used for each row to the
     * data on load and then sorting on those nonces before writing out the data. Instead of sorting on Nonces created using Java's
     * {@code SecureRandom}, Spark is using its own {@code rand()} function for the shuffle.
     *
     * @param rawInputData The Dataset to shuffle
     * @return The shuffled Dataset
     */
    static Dataset<Row> shuffleData(final Dataset<Row> rawInputData) {
        return rawInputData.orderBy(functions.rand());
    }

    /**
     * Shut down the Spark session.
     *
     * @param spark the SparkSession to close
     */
    private static void closeSparkSession(final SparkSession spark) {
        spark.stop();
    }
}
