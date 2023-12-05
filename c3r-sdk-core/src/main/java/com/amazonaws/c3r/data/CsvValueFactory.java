// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Factory for creating empty CSV rows.
 */
public class CsvValueFactory implements ValueFactory<CsvValue> {
    /**
     * Takes an encoded value and recreates it as a string regardless of data type.
     *
     * @param bytes Encoded value
     * @return String representing value
     */
    @Override
    public CsvValue createValueFromEncodedBytes(final byte[] bytes) {
        final ClientDataType type = ValueConverter.clientDataTypeForEncodedValue(bytes);
        switch (type) {
            case BIGINT:
                final Long bigint = ValueConverter.BigInt.decode(bytes);
                final String bigintStr = (bigint == null) ? null : bigint.toString();
                return new CsvValue(bigintStr);
            case BOOLEAN:
                final Boolean bool = ValueConverter.Boolean.decode(bytes);
                final String boolStr = (bool == null) ? null :
                        bool ? "true" : "false";
                return new CsvValue(boolStr);
            case CHAR:
                return new CsvValue(ValueConverter.Char.decode(bytes));
            case DATE:
                final Integer date = ValueConverter.Date.decode(bytes);
                return new CsvValue(convertDateToString(date));
            case DECIMAL:
                final ClientValueWithMetadata.Decimal decimal = ValueConverter.Decimal.decode(bytes);
                final String decimalStr = (decimal.getValue() == null) ? null : decimal.getValue().toPlainString();
                return new CsvValue(decimalStr);
            case DOUBLE:
                final Double dbl = ValueConverter.Double.decode(bytes);
                final String dblStr = (dbl == null) ? null : dbl.toString();
                return new CsvValue(dblStr);
            case FLOAT:
                final Float flt = ValueConverter.Float.decode(bytes);
                final String fltStr = (flt == null) ? null : flt.toString();
                return new CsvValue(fltStr);
            case INT:
                final Integer integer = ValueConverter.Int.decode(bytes);
                final String intStr = (integer == null) ? null : integer.toString();
                return new CsvValue(intStr);
            case SMALLINT:
                final Short shrt = ValueConverter.SmallInt.decode(bytes);
                final String shrtStr = (shrt == null) ? null : shrt.toString();
                return new CsvValue(shrtStr);
            case STRING:
                return new CsvValue(ValueConverter.String.decode(bytes));
            case TIMESTAMP:
                final ClientValueWithMetadata.Timestamp timestamp = ValueConverter.Timestamp.decode(bytes);
                return new CsvValue(convertTimestampToString(timestamp));
            case VARCHAR:
                final ClientValueWithMetadata.Varchar varchar = ValueConverter.Varchar.decode(bytes);
                return new CsvValue(varchar.getValue());
            default:
                throw new C3rRuntimeException("Unable to decode bytes to a valid type.");
        }
    }

    /**
     * Date in ISO-8601 format: yyyy-MM-dd.
     *
     * @param date Days relative to epoch
     * @return yyyy-MM-dd formatted date
     */
    private String convertDateToString(final Integer date) {
        if (date == null) {
            return null;
        }
        return LocalDate.ofEpochDay(date.longValue()).toString();
    }

    /**
     * Timestamp in ISO-8601 format: yyyy-MM-dd HH:mm:ss.SSSSSS.
     *
     * @param timestamp Timestamp value and metadata
     * @return Formatted Timestamp String
     * @throws C3rRuntimeException if unexpected time unit is found
     */
    private String convertTimestampToString(final ClientValueWithMetadata.Timestamp timestamp) {
        if (timestamp.getValue() == null) {
            return null;
        }

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        final ZoneId zone = timestamp.getIsUtc() ? ZoneOffset.UTC : ZoneId.systemDefault();

        if (timestamp.getUnit() == Units.Seconds.MILLIS) {
            return formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(timestamp.getValue(), ChronoUnit.MILLIS), zone));
        } else if (timestamp.getUnit() == Units.Seconds.MICROS) {
            return formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(timestamp.getValue(), ChronoUnit.MICROS), zone));
        } else if (timestamp.getUnit() == Units.Seconds.NANOS) {
            return formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(timestamp.getValue(), ChronoUnit.NANOS), zone));
        } else {
            throw new C3rRuntimeException("Unexpected time unit found.");
        }
    }

    /**
     * Takes an encoded value and returns the bytes representing the type as a UTF-8 string.
     *
     * @param bytes Encoded value
     * @return UTF-8 encoded string bytes
     */
    @Override
    public byte[] getValueBytesFromEncodedBytes(final byte[] bytes) {
        return createValueFromEncodedBytes(bytes).getBytes();
    }

    /**
     * Creates an empty CSV row for storing data.
     *
     * @return Empty CSV value row
     */
    public CsvRow newRow() {
        return new CsvRow();
    }
}
