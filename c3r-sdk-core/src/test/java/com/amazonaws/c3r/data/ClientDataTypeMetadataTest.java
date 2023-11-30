// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientDataTypeMetadataTest {
    @Test
    public void decimalConstructionTest() {
        final Integer scale = 2;
        final Integer precision = 3;
        final BigDecimal value = BigDecimal.valueOf(101, scale);

        final ClientValueWithMetadata.Decimal allSpecified = new ClientValueWithMetadata.Decimal(value, precision, scale);
        assertEquals(scale, allSpecified.getScale());
        assertEquals(precision, allSpecified.getPrecision());
        assertEquals(value, allSpecified.getValue());

        final ClientValueWithMetadata.Decimal valueMissing = new ClientValueWithMetadata.Decimal(null, precision, scale);
        assertEquals(scale, valueMissing.getScale());
        assertEquals(precision, valueMissing.getPrecision());
        assertNull(valueMissing.getValue());

        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Decimal(value, null, scale));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Decimal(value, precision, null));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Decimal(null, null, scale));

        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Decimal(null, precision, null));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Decimal(value, null, null));

        final ClientValueWithMetadata.Decimal allNull = new ClientValueWithMetadata.Decimal(null, null, null);
        assertNull(allNull.getScale());
        assertNull(allNull.getPrecision());
        assertNull(allNull.getValue());
    }

    @Test
    public void timestampConstructionTest() {
        final Long time = 100L;
        final Boolean isUtc = true;
        final Units.Seconds units = Units.Seconds.MILLIS;

        final ClientValueWithMetadata.Timestamp allSpecified = new ClientValueWithMetadata.Timestamp(time, isUtc, units);
        assertEquals(time, allSpecified.getValue());
        assertEquals(isUtc, allSpecified.getIsUtc());
        assertEquals(units, allSpecified.getUnit());

        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Timestamp(time, null, units));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Timestamp(time, isUtc, null));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Timestamp(time, null, null));

        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Timestamp(null, null, units));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Timestamp(null, isUtc, null));

        final ClientValueWithMetadata.Timestamp nullValueWithMetadata = new ClientValueWithMetadata.Timestamp(null, isUtc, units);
        assertNull(nullValueWithMetadata.getValue());
        assertEquals(isUtc, nullValueWithMetadata.getIsUtc());
        assertEquals(units, nullValueWithMetadata.getUnit());

        final ClientValueWithMetadata.Timestamp nullValueWithoutMetadata = new ClientValueWithMetadata.Timestamp(null, null, null);
        assertNull(nullValueWithoutMetadata.getValue());
        assertNull(nullValueWithoutMetadata.getIsUtc());
        assertNull(nullValueWithoutMetadata.getUnit());
    }

    @Test
    public void varcharConstructionTest() {
        final String value = "hello";
        final String longValue = "hello world!";
        final Integer maxLength = 10;

        final ClientValueWithMetadata.Varchar allSpecified = new ClientValueWithMetadata.Varchar(value, maxLength);
        assertEquals(value, allSpecified.getValue());
        assertEquals(maxLength, allSpecified.getMaxLength());

        final ClientValueWithMetadata.Varchar nullValue = new ClientValueWithMetadata.Varchar(null, maxLength);
        assertNull(nullValue.getValue());
        assertEquals(maxLength, nullValue.getMaxLength());

        final ClientValueWithMetadata.Varchar allNull = new ClientValueWithMetadata.Varchar(null, null);
        assertNull(allNull.getValue());
        assertNull(allNull.getMaxLength());

        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Varchar(value, null));
        assertThrows(C3rIllegalArgumentException.class, () -> new ClientValueWithMetadata.Varchar(longValue, maxLength));
    }
}