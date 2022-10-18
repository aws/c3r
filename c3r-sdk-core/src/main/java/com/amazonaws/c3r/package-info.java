// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Handles the transformation of cleartext data into encrypted data.
 *
 * <p>
 * This package takes in the settings for a clean room, a data file and then preforms the necessary transformations to create the
 * encrypted output. The row marshallers and unmarshallers (see {@link com.amazonaws.c3r.action}) are the main entry point into the SDK.
 * To encrypt data, create a new instance of a {@link com.amazonaws.c3r.action.RowMarshaller} for the particular data format you are using,
 * then calling {@link com.amazonaws.c3r.action.RowMarshaller#marshal()} and {@link com.amazonaws.c3r.action.RowMarshaller#close()} will
 * transform the cleartext data according to the schema into a file containing the encrypted data. To decrypt data, an instance of a
 * {@link com.amazonaws.c3r.action.RowUnmarshaller} for the particular data type is created, then
 * {@link com.amazonaws.c3r.action.RowUnmarshaller#unmarshal()} and {@link com.amazonaws.c3r.action.RowUnmarshaller#close()} are called.
 *
 * <p>
 * The settings are stored in an instance of {@link com.amazonaws.c3r.config.EncryptConfig} or
 * {@link com.amazonaws.c3r.config.DecryptConfig} for the respective mode of operation. The schema information is kept in an instance of
 * the {@link com.amazonaws.c3r.config.TableSchema} which is backed by several implementations. Between the configuration and schema
 * classes, the row marshallers and unmarshallers will have the information they need to do cryptographic transforms.
 *
 * <p>
 * The rest of the packages and classes inside this package are in support of these top level classes. Classes in the package
 * {@code com.amazonaws.config} will be used in the course of creating the cryptographic configurations but the remaining classes are not
 * meant to be used directly for development as they may change at will.
 *
 * <p>
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.c3r;

