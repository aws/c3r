// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.examples;

import com.amazonaws.c3r.action.ParquetRowMarshaller;
import com.amazonaws.c3r.action.ParquetRowUnmarshaller;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.action.RowUnmarshaller;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.io.FileFormat;

import java.util.List;

/**
 * Example code for creating a schema, encrypting and decrypting Parquet data.
 */
public final class ParquetExample {
    /**
     * An example 32-byte key used for testing.
     */
    private static final String EXAMPLE_SHARED_SECRET_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFrEXAMPLESECRET=";

    /**
     * An example salt for testing.
     */
    private static final String EXAMPLE_SALT = "00000000-1111-2222-3333-444444444444";

    /**
     * Generates a table schema. The input file has a header row which contains (at least) the following columns
     * (case-insensitive, leading and trailing whitespace are ignored):
     * <ul>
     *     <li>firstname</li>
     *     <li>lastname</li>
     *     <li>address</li>
     *     <li>city</li>
     *     <li>state</li>
     *     <li>phonenumber</li>
     *     <li>title</li>
     *     <li>level</li>
     *     <li>notes</li>
     * </ul>
     */
    private static final TableSchema EXAMPLE_TABLE_SCHEMA = new MappedTableSchema(List.of(
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("firstname"))
                    .targetHeader(new ColumnHeader("fname"))
                    .type(ColumnType.CLEARTEXT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("lastname"))
                    .targetHeader(new ColumnHeader("lname"))
                    .type(ColumnType.CLEARTEXT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("address"))
                    .targetHeader(new ColumnHeader("address"))
                    .pad(Pad.builder().type(PadType.MAX).length(32).build())
                    .type(ColumnType.SEALED)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("city"))
                    .targetHeader(new ColumnHeader("city"))
                    .pad(Pad.builder().type(PadType.MAX).length(16).build())
                    .type(ColumnType.SEALED)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("state"))
                    .targetHeader(new ColumnHeader("state"))
                    .type(ColumnType.FINGERPRINT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("phonenumber"))
                    .targetHeader(new ColumnHeader("phonenumber_cleartext"))
                    .pad(null)
                    .type(ColumnType.CLEARTEXT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("phonenumber"))
                    .targetHeader(new ColumnHeader("phonenumber_sealed"))
                    .pad(Pad.DEFAULT)
                    .type(ColumnType.SEALED)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("phonenumber"))
                    .targetHeader(new ColumnHeader("phonenumber_fingerprint"))
                    .type(ColumnType.FINGERPRINT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("title"))
                    .targetHeader(new ColumnHeader("title"))
                    .pad(Pad.builder().type(PadType.FIXED).length(128).build())
                    .type(ColumnType.SEALED)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("level"))
                    .targetHeader(new ColumnHeader("level"))
                    .pad(null)
                    .type(ColumnType.CLEARTEXT)
                    .build(),
            ColumnSchema.builder()
                    .sourceHeader(new ColumnHeader("notes"))
                    .targetHeader(new ColumnHeader("notes"))
                    .pad(Pad.builder().type(PadType.MAX).length(100).build())
                    .type(ColumnType.SEALED)
                    .build()
    ));

    /**
     * Hidden example class constructor.
     */
    private ParquetExample() {
    }

    /**
     * Demonstrates encrypting a file. Uses the following columns with a predetermined schema, shared secret key, and collaboration ID:
     * <ul>
     *   <li>firstname</li>
     *   <li>lastname</li>
     *   <li>address</li>
     *   <li>city</li>
     *   <li>state</li>
     *   <li>phonenumber</li>
     *   <li>title</li>
     *   <li>level</li>
     *   <li>notes</li>
     *   </ul>
     *
     * @param sourceFile Source CSV file matching aforementioned schema
     * @param targetFile Destination for encrypted table
     */
    public static void encrypt(final String sourceFile,
                        final String targetFile) {
        final var encryptionConfig = EncryptConfig.builder()
                .sourceFile(sourceFile)
                .targetFile(targetFile)
                .fileFormat(FileFormat.PARQUET)
                .secretKey(KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY))
                .salt(EXAMPLE_SALT)
                .tempDir(".")
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(EXAMPLE_TABLE_SCHEMA)
                .overwrite(true)
                .build();

        final RowMarshaller<ParquetValue> parquetRowMarshaller =
                ParquetRowMarshaller.newInstance(encryptionConfig);
        parquetRowMarshaller.marshal();
        parquetRowMarshaller.close();
    }

    /**
     * Decrypt an encrypted table for a predetermined shared secret key, and collaboration ID.
     *
     * @param sourceFile Encrypted table to decrypt
     * @param targetFile Where to store decrypted results
     */
    public static void decrypt(final String sourceFile,
                        final String targetFile) {
        final var decryptConfig = DecryptConfig.builder()
                .sourceFile(sourceFile)
                .targetFile(targetFile)
                .fileFormat(FileFormat.PARQUET)
                .secretKey(KeyUtil.sharedSecretKeyFromString(EXAMPLE_SHARED_SECRET_KEY))
                .salt(EXAMPLE_SALT)
                .overwrite(true)
                .build();

        final RowUnmarshaller<ParquetValue> parquetRowUnmarshaller =
                ParquetRowUnmarshaller.newInstance(decryptConfig);
        parquetRowUnmarshaller.unmarshal();
        parquetRowUnmarshaller.close();
    }
}
