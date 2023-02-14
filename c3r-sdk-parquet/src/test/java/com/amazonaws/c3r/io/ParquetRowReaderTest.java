// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetRow;
import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.ParquetTestUtility;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetRowReaderTest {

    public static Boolean getBoolean(final Row<ParquetValue> row) {
        return ((ParquetValue.Boolean) row.getValue(new ColumnHeader("boolean"))).getValue();
    }

    public static String getString(final Row<ParquetValue> row) {
        return new String(((ParquetValue.Binary) row.getValue(new ColumnHeader("string"))).getValue().getBytes(),
                StandardCharsets.UTF_8);
    }

    public static Integer getInt8(final Row<ParquetValue> row) {
        return ((ParquetValue.Int) row.getValue(new ColumnHeader("int8"))).getValue();
    }

    public static Integer getInt16(final Row<ParquetValue> row) {
        return ((ParquetValue.Int) row.getValue(new ColumnHeader("int16"))).getValue();
    }

    public static Integer getInt32(final Row<ParquetValue> row) {
        return ((ParquetValue.Int) row.getValue(new ColumnHeader("int32"))).getValue();
    }

    public static Long getInt64(final Row<ParquetValue> row) {
        return ((ParquetValue.Long) row.getValue(new ColumnHeader("int64"))).getValue();
    }

    public static Float getFloat(final Row<ParquetValue> row) {
        return ((ParquetValue.Float) row.getValue(new ColumnHeader("float"))).getValue();
    }

    public static Double getDouble(final Row<ParquetValue> row) {
        return ((ParquetValue.Double) row.getValue(new ColumnHeader("double"))).getValue();
    }

    public static Long getTimestamp(final Row<ParquetValue> row) {
        return ((ParquetValue.Long) row.getValue(new ColumnHeader("timestamp"))).getValue();
    }

    public static void validateRowsGetValueContent(final List<Row<ParquetValue>> rows) {
        for (int i = 0; i < rows.size(); i++) {
            final var row = rows.get(i);
            assertEquals(i % 2 == 0, getBoolean(row), "row " + i);
            assertEquals(String.valueOf(i), getString(row), "row " + i);
            assertEquals((8 + i) % 127, getInt8(row), "row " + i);
            assertEquals((16 + i) % 32767, getInt16(row), "row " + i);
            assertEquals((32 + i), getInt32(row), "row " + i);
            assertEquals((64 + i), getInt64(row), "row " + i);
            assertEquals((float) (1.0 + i), getFloat(row), 0.000001, "row " + i);
            assertEquals(-1.0 - i, getDouble(row), 0.000001, "row " + i);
            assertEquals(-446774400000000L, getTimestamp(row), "row " + i);
        }
    }

    public static void validateRowsGetValueNullContent(final List<Row<ParquetValue>> rows) {
        for (var row : rows) {
            for (var header : ParquetTestUtility.PARQUET_TEST_DATA_HEADERS) {
                assertTrue(row.getValue(header).isNull());
                assertNull(row.getValue(header).getBytes());
            }
        }
    }

    @Test
    public void getSourceNameTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        assertEquals(
                ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH,
                pReader.getSourceName());
    }

    @Test
    public void getHeadersTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        assertEquals(
                ParquetTestUtility.PARQUET_TEST_DATA_HEADERS,
                pReader.getHeaders());
    }

    @Test
    public void getReadRowCountTest() {
        var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        assertEquals(0, pReader.getReadRowCount());
        ParquetTestUtility.readAllRows(pReader);
        assertEquals(1, pReader.getReadRowCount());

        pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_100_ROWS_PRIM_DATA_PATH);
        ParquetTestUtility.readAllRows(pReader);
        assertEquals(100, pReader.getReadRowCount());
    }

    @Test
    public void getParquetSchemaSupportsCryptoComputingTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        final var schema = pReader.getParquetSchema();
        ParquetTestUtility.PARQUET_TEST_DATA_TYPES.forEach((header, type) ->
                assertEquals(type, schema.getColumnType(header).getClientDataType(),
                        "column `" + header + "` has the wrong type"));
    }

    @Test
    public void nextTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        final var row = pReader.next();
        assertEquals(ParquetTestUtility.PARQUET_TEST_ROW_TYPE_ENTRIES.size(), row.size());
        for (var header : ParquetTestUtility.PARQUET_TEST_DATA_HEADERS) {
            assertTrue(row.hasColumn(header));
        }
    }

    @Test
    public void validate1RowGetValueTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        final var rows = ParquetTestUtility.readAllRows(pReader);
        assertEquals(rows.size(), 1);
        validateRowsGetValueContent(rows);
    }

    @Test
    public void validate100RowsGetValueTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_100_ROWS_PRIM_DATA_PATH);
        final var rows = ParquetTestUtility.readAllRows(pReader);
        assertEquals(rows.size(), 100);
        validateRowsGetValueContent(rows);
    }

    @Test
    public void validate100RowsIn10GroupsGetValueTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_100_ROWS_10_GROUPS_PRIM_DATA_PATH);
        final var rows = ParquetTestUtility.readAllRows(pReader);
        assertEquals(rows.size(), 100);
        validateRowsGetValueContent(rows);

    }

    @Test
    public void validate1NullRowGetValueTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_NULL_1_ROW_PRIM_DATA_PATH);
        final var rows = ParquetTestUtility.readAllRows(pReader);
        assertEquals(rows.size(), 1);
        validateRowsGetValueNullContent(rows);
    }

    @Test
    public void validate100NullRowsGetValueTest() {
        final var pReader = new ParquetRowReader(ParquetTestUtility.PARQUET_NULL_100_ROWS_PRIM_DATA_PATH);
        final var rows = ParquetTestUtility.readAllRows(pReader);
        assertEquals(rows.size(), 100);
        validateRowsGetValueNullContent(rows);
    }

    @Test
    public void maxColumnCountTest() throws IOException {
        final List<Type> types = new ArrayList<>();
        final Map<ColumnHeader, ParquetDataType> columnTypes = new HashMap<>();
        for (int i = 0; i < ParquetRowReader.MAX_COLUMN_COUNT + 1; i++) {
            final Type type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("string" + i);
            columnTypes.put(new ColumnHeader("string" + i), ParquetDataType.fromType(type));
            types.add(type);
        }
        final var messageType = new MessageType("Oversized", types);
        final ParquetSchema parquetSchema = new ParquetSchema(messageType);

        final String output = FileTestUtility.createTempFile("output", ".parquet").toString();
        final ParquetRowWriter writer = ParquetRowWriter.builder()
                .targetName(output)
                .parquetSchema(parquetSchema)
                .build();
        final Row<ParquetValue> row = new ParquetRow(columnTypes);
        writer.writeRow(row);
        writer.flush();
        writer.close();
        assertThrows(C3rRuntimeException.class, () -> new ParquetRowReader(output));
    }

}
