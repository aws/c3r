// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples.csv;

import com.amazonaws.c3r.action.CsvRowMarshaller;
import com.amazonaws.c3r.action.CsvRowUnmarshaller;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.action.RowUnmarshaller;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.io.FileFormat;

import java.util.List;

/**
 * Examples of encrypting and decrypting a CSV file with no headers in the input files.
 */
public final class CsvNoHeaderExample {
    /**
     * An example 32-byte key used for testing.
     */
    private static final String EXAMPLE_SHARED_SECRET_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFrEXAMPLESECRET=";

    /**
     * Example collaboration ID, i.e., the value used by all participating parties as a salt for encryption.
     */
    private static final String EXAMPLE_SALT = "00000000-1111-2222-3333-444444444444";

    /**
     * Table schema for an input CSV file with no header row and exactly 9 columns. Each List of
     * ColumnSchema indicates how many output columns that positional input column should be mapped to.
     */
    private static final TableSchema EXAMPLE_TABLE_SCHEMA = new PositionalTableSchema(List.of(
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("fname"))
                    .type(ColumnType.CLEARTEXT)
                    .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("lname"))
                    .type(ColumnType.CLEARTEXT)
                    .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("address"))
                    .pad(Pad.builder().type(PadType.MAX).length(32).build())
                    .type(ColumnType.SEALED)
                    .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("city"))
                    .pad(Pad.builder().type(PadType.MAX).length(16).build())
                    .type(ColumnType.SEALED)
                    .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("state"))
                    .type(ColumnType.FINGERPRINT)
                    .build()),
            // We map a single input column to multiple output columns by providing a list with
            // the desired number of ColumnSchema in that column's position.
            List.of(ColumnSchema.builder()
                            .targetHeader(new ColumnHeader("phonenumber_cleartext"))
                            .pad(null)
                            .type(ColumnType.CLEARTEXT)
                            .build(),
                    ColumnSchema.builder()
                            .targetHeader(new ColumnHeader("phonenumber_sealed"))
                            .pad(Pad.DEFAULT)
                            .type(ColumnType.SEALED)
                            .build(),
                    ColumnSchema.builder()
                            .targetHeader(new ColumnHeader("phonenumber_fingerprint"))
                            .type(ColumnType.FINGERPRINT)
                            .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("title"))
                    .pad(Pad.builder().type(PadType.FIXED).length(128).build())
                    .type(ColumnType.SEALED)
                    .build()),
            List.of(ColumnSchema.builder()
                    .targetHeader(new ColumnHeader("level"))
                    .pad(null)
                    .type(ColumnType.CLEARTEXT)
                    .build()),
            // We omit the last column from our encrypted table by providing an
            // empty list of ColumnSchema at that position.
            List.of()
    ));

    /**
     * Hidden demo class constructor.
     */
    private CsvNoHeaderExample() {
    }

    /**
     * Encrypts a CSV file with no header row and exactly 9 columns according to a predetermined schema.
     *
     * @param sourceFile Source CSV file with no header row and exactly 9 columns
     * @param targetFile Destination for encrypted table
     */
    public static void encrypt(final String sourceFile,
                        final String targetFile) {
        final var encryptionConfig = EncryptConfig.builder()
                .sourceFile(sourceFile)
                .targetFile(targetFile)
                .fileFormat(FileFormat.CSV)
                .secretKey(KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY))
                .salt(EXAMPLE_SALT)
                .tempDir(".")
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(EXAMPLE_TABLE_SCHEMA)
                .overwrite(true)
                .build();

        final RowMarshaller<CsvValue> csvRowMarshaller =
                CsvRowMarshaller.newInstance(encryptionConfig);
        csvRowMarshaller.marshal();
        csvRowMarshaller.close();
    }

    /**
     * Decrypt an encrypted table for a predetermined shared secret key, and salt.
     *
     * @param sourceFile Encrypted table to decrypt
     * @param targetFile Where to store decrypted results
     */
    public static void decrypt(final String sourceFile,
                        final String targetFile) {
        final var decryptConfig = DecryptConfig.builder()
                .sourceFile(sourceFile)
                .targetFile(targetFile)
                .fileFormat(FileFormat.CSV)
                .secretKey(KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY))
                .salt(EXAMPLE_SALT)
                .overwrite(true)
                .build();

        final RowUnmarshaller<CsvValue> csvRowUnmarshaller =
                CsvRowUnmarshaller.newInstance(decryptConfig);
        csvRowUnmarshaller.unmarshal();
        csvRowUnmarshaller.close();
    }
}
