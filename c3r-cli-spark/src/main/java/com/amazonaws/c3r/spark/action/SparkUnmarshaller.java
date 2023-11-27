// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.action;

import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.spark.config.SparkDecryptConfig;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.jdk.CollectionConverters;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Spark orchestration of the C3R SDK for decryption.
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
public abstract class SparkUnmarshaller {

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
     * @param inputData     input Dataset to process
     * @param decryptConfig Decryption config to use for processing
     * @return The decrypted Dataset
     */
    public static Dataset<Row> decrypt(final Dataset<Row> inputData, final SparkDecryptConfig decryptConfig) {
        return unmarshalData(inputData, decryptConfig);
    }

    /**
     * Decrypt source data.
     *
     * @param rawInputData  The source data to be decrypted
     * @param decryptConfig Decryption config to use for processing
     * @return The cleartext data
     * @throws C3rRuntimeException If the input data cannot be unmarshalled.
     */
    static Dataset<Row> unmarshalData(final Dataset<Row> rawInputData, final SparkDecryptConfig decryptConfig) {
        // Copy out values that need to be serialized
        final String salt = decryptConfig.getSalt();
        final String base64EncodedKey = Base64.getEncoder().encodeToString(decryptConfig.getSecretKey().getEncoded());
        final boolean failOnFingerprintColumns = decryptConfig.isFailOnFingerprintColumns();

        final ExpressionEncoder<Row> rowEncoder = ExpressionEncoder.apply(rawInputData.schema());
        final StructField[] fields = rawInputData.schema().fields();
        try {
            return rawInputData.map((MapFunction<Row, Row>) row -> {
                // Build a list of transformers for each row, limiting state to keys/salts/settings POJOs
                final Map<ColumnType, Transformer> transformers = Transformer.initTransformers(
                        KeyUtil.sharedSecretKeyFromString(base64EncodedKey),
                        salt,
                        null,  // Not relevant to decryption.
                        failOnFingerprintColumns);
                // For each column in the row, transform the data
                final List<Object> unmarshalledValues = new ArrayList<>();
                for (int i = 0; i < row.size(); i++) {
                    // Pass through non-String data types
                    if (fields[i].dataType() != DataTypes.StringType) {
                        unmarshalledValues.add(row.get(i));
                        continue;
                    }
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
        } catch (Exception e) {
            throw new C3rRuntimeException("Unknown exception when decrypting data.", e);
        }
    }
}
