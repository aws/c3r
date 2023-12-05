// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

/**
 * Factory for creating empty Parquet rows with the given column types.
 */
public class ParquetValueFactory implements ValueFactory<ParquetValue> {
    /**
     * Map column name to type.
     */
    private final Map<ColumnHeader, ParquetDataType> columnTypes;

    /**
     * Initializes the factory with type information for the row.
     *
     * @param columnTypes Data type associated with a column name
     */
    public ParquetValueFactory(final Map<ColumnHeader, ParquetDataType> columnTypes) {
        this.columnTypes = applyIfNonNull(Map::copyOf, columnTypes);
    }

    /**
     * Apply a function to a value only if the value isn't {@code null}.
     *
     * @param function Computation on value
     * @param value    Value
     * @param <T>      Input Class
     * @param <R>      Result Class
     * @return         {@code null} if {@code value} is {@code null}, result of function otherwise
     */
    private <T, R> R applyIfNonNull(final Function<T, R> function, final T value) {
        if (value != null) {
            return function.apply(value);
        }
        return null;
    }

    /**
     * Take a Java string and convert it to its binary UTF-8 representation and store in a Parquet Binary value.
     *
     * @param str String to encode in Parquet Binary format
     * @return Parquet representation of string
     */
    private Binary createParquetBinaryFromString(final String str) {
        return applyIfNonNull(Binary::fromReusedByteArray, applyIfNonNull((String s) -> s.getBytes(StandardCharsets.UTF_8), str));
    }

    /**
     * Take a byte array encoding a BigInt value and convert it to a Parquet Int64 value.
     *
     * @param bytes Encoded BigInt value
     * @return Parquet Int64 value
     */
    private ParquetValue.Int64 getBigInt(final byte[] bytes) {
        final Long bigint = ValueConverter.BigInt.decode(bytes);
        return new ParquetValue.Int64(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .named("")),
                bigint);
    }

    /**
     * Take a byte array encoding a Boolean value and convert it to a Parquet Boolean value.
     *
     * @param bytes Encoded Boolean value
     * @return Parquet Boolean value
     */
    private ParquetValue.Boolean getBoolean(final byte[] bytes) {
        final Boolean bool = ValueConverter.Boolean.decode(bytes);
        return new ParquetValue.Boolean(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                                .named("")),
                bool);
    }

    /**
     * Take a byte array encoding a Char, String or Varchar value and convert it to a Parquet String value.
     *
     * @param decode Function to turn bytes into a Java String
     * @param bytes  Encoded Char/String/Varchar value
     * @return Parquet String value
     */
    private ParquetValue.Binary getStringType(final Function<byte[], String> decode, final byte[] bytes) {
        final String str = decode.apply(bytes);
        return new ParquetValue.Binary(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("")),
                createParquetBinaryFromString(str));
    }

    /**
     * Take a byte array encoding a Date value and convert it to a Parquet Date value.
     *
     * @param bytes Encoded Date value
     * @return Parquet Date value
     */
    private ParquetValue.Int32 getDate(final byte[] bytes) {
        final Integer date = ValueConverter.Date.decode(bytes);
        return new ParquetValue.Int32(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.dateType()).named("")),
                date);
    }

    /**
     * Take a byte array encoding a Decimal value and convert it to a Parquet Binary Decimal value.
     * All Decimals are converted to Parquet Binary values.
     *
     * @param bytes Encoded Decimal value
     * @return Parquet Binary Decimal value
     */
    private ParquetValue.Binary getDecimal(final byte[] bytes) {
        final ClientValueWithMetadata.Decimal decimal = ValueConverter.Decimal.decode(bytes);
        if (decimal.getValue() == null && (decimal.getScale() == null || decimal.getPrecision() == null)) {
            return new ParquetValue.Binary(
                    ParquetDataType.fromType(
                            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                    .as(LogicalTypeAnnotation.decimalType(0, 1))
                                    .named("")),
                    null);
        }
        return new ParquetValue.Binary(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.decimalType(decimal.getScale(), decimal.getPrecision()))
                                .named("")),
                Binary.fromReusedByteArray(decimal.getValue().unscaledValue().toByteArray()));
    }

    /**
     * Take a byte array encoding a Double value and convert it to a Parquet Double value.
     *
     * @param bytes Encoded Double value
     * @return Parquet Double value
     */
    final ParquetValue.Double getDouble(final byte[] bytes) {
        final Double dbl = ValueConverter.Double.decode(bytes);
        return new ParquetValue.Double(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                                .named("")),
                dbl);
    }

    /**
     * Take a byte array encoding a Float value and convert it to a Parquet Float value.
     *
     * @param bytes Encoded Float value
     * @return Parquet Float value
     */
    final ParquetValue.Float getFloat(final byte[] bytes) {
        final Float flt = ValueConverter.Float.decode(bytes);
        return new ParquetValue.Float(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                                .named("")),
                flt);
    }

    /**
     * Take a byte array encoding an integer value and convert it to a Parquet Int32 value.
     *
     * @param bytes Encoded Int value
     * @return Parquet Int32 value
     */
    private ParquetValue.Int32 getInt(final byte[] bytes) {
        final Integer integer = ValueConverter.Int.decode(bytes);
        return new ParquetValue.Int32(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("")),
                integer);
    }

    /**
     * Take a byte array encoding a SmallInt value and convert it to a Parquet Int32 value containing a 16 bit value.
     *
     * @param bytes Encoded SmallInt value
     * @return Parquet Int32 value
     */
    private ParquetValue.Int32 getSmallInt(final byte[] bytes) {
        final Short shrt = ValueConverter.SmallInt.decode(bytes);
        return new ParquetValue.Int32(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.intType(ClientDataType.SMALLINT_BIT_SIZE, true))
                                .named("")),
                applyIfNonNull(Short::intValue, shrt));
    }

    /**
     * Take a byte array encoding a timestamp value and convert it to a Parquet Timestamp value.
     *
     * @param bytes Encoded Timestamp value
     * @return Parquet Timestamp value
     * @throws C3rRuntimeException if an unexpected time unit is found
     */
    private ParquetValue.Int64 getTimestamp(final byte[] bytes) {
        final ClientValueWithMetadata.Timestamp timestamp = ValueConverter.Timestamp.decode(bytes);
        if (timestamp.getValue() == null && timestamp.getUnit() == null) {
            return new ParquetValue.Int64(
                    ParquetDataType.fromType(
                            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                    .as(LogicalTypeAnnotation.timestampType(false, null))
                                    .named("")),
                    null);
        }
        final LogicalTypeAnnotation.TimeUnit unit;
        switch (timestamp.getUnit()) {
            case MILLIS:
                unit = LogicalTypeAnnotation.TimeUnit.MILLIS;
                break;
            case MICROS:
                unit = LogicalTypeAnnotation.TimeUnit.MICROS;
                break;
            case NANOS:
                unit = LogicalTypeAnnotation.TimeUnit.NANOS;
                break;
            default:
                throw new C3rRuntimeException("Unexpected time type.");
        }
        return new ParquetValue.Int64(
                ParquetDataType.fromType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(timestamp.getIsUtc(), unit))
                                .named("")),
                timestamp.getValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ParquetValue createValueFromEncodedBytes(final byte[] bytes) {
        final ClientDataType type = ValueConverter.clientDataTypeForEncodedValue(bytes);
        switch (type) {
            case BIGINT:
                return getBigInt(bytes);
            case BOOLEAN:
                return getBoolean(bytes);
            case CHAR:
                return getStringType(ValueConverter.Char::decode, bytes);
            case DATE:
                return getDate(bytes);
            case DECIMAL:
                return getDecimal(bytes);
            case DOUBLE:
                return getDouble(bytes);
            case FLOAT:
                return getFloat(bytes);
            case INT:
                return getInt(bytes);
            case SMALLINT:
                return getSmallInt(bytes);
            case STRING:
                return getStringType(ValueConverter.String::decode, bytes);
            case TIMESTAMP:
                return getTimestamp(bytes);
            case VARCHAR:
                return getStringType((byte[] b) -> ValueConverter.Varchar.decode(b).getValue(), bytes);
            default:
                throw new C3rRuntimeException("Unable to decode bytes to a valid type.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getValueBytesFromEncodedBytes(final byte[] bytes) {
        return createValueFromEncodedBytes(bytes).getBytes();
    }

    /**
     * Create an empty row to be populated by the callee.
     *
     * @return An empty Row for storing data in
     */
    @Override
    public Row<ParquetValue> newRow() {
        return new ParquetRow(columnTypes);
    }
}
