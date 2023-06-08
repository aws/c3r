// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * This package classes used to marshall (encrypt) and unmarshall (decrypt) data to and from the clean room for the various supported
 * data types. {@link com.amazonaws.c3r.spark.action.SparkMarshaller} handles the logic of marshalling data outside of anything having to
 * do with the actual data format and {@link com.amazonaws.c3r.spark.action.SparkUnmarshaller} does the same for unmarshalling. Each format
 * specific class handles file I/O and value creation only for that particular data type.
 *
 * <p>
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.c3r.spark.action;