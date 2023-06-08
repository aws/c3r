// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.io.CsvRowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A partition reader returned by {@code PartitionReaderFactory.createReader(InputPartition)} or {@code PartitionReaderFactory
 * .createColumnarReader(InputPartition)}. It's responsible for outputting data for a RDD partition.
 */
public class CsvPartitionReader implements PartitionReader<InternalRow> {

    /**
     * Reader for processing CSV files.
     */
    private final CsvRowReader csvReader;

    /**
     * Constructs a new CsvPartitionReader.
     *
     * @param properties A map of configuration settings
     */
    public CsvPartitionReader(final Map<String, String> properties) {
        this.csvReader = SparkCsvReader.initReader(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean next() {
        return csvReader.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InternalRow get() {
        final Row<CsvValue> row = csvReader.next();
        final List<ColumnHeader> headers = csvReader.getHeaders();
        final Object[] data = new Object[row.size()];
        for (int i = 0; i < data.length; i++) {
            final CsvValue val = row.getValue(headers.get(i));
            data[i] = val.isNull() ? null : UTF8String.fromString(val.toString());
        }
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(data).iterator()).asScala().toSeq());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        csvReader.close();
    }
}
