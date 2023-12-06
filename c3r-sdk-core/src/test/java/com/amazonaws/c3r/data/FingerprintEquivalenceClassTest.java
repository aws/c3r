// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FingerprintEquivalenceClassTest {
    private final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);

    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, "AES");

    private FingerprintTransformer fingerprintTransformer;

    private EncryptionContext context(final ClientDataType type) {
        return EncryptionContext.builder()
                .columnLabel("label")
                .clientDataType(type)
                .build();
    }

    @BeforeEach
    public void setup() {
        fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.highAssuranceMode(), false);
    }

    @Test
    public void bigintIsAccepted() {
        final byte[] lVal = ValueConverter.BigInt.toBytes(Long.MAX_VALUE);
        final var results = fingerprintTransformer.marshal(lVal, context(ClientDataType.BIGINT));
        assertArrayEquals("02:hmac:kjXoghmHSYZn5nBkoWZpyAlIyBmao03MAVIdHp4rSy4=".getBytes(StandardCharsets.UTF_8), results);
    }

    @Test
    public void booleanIsAccepted() {
        final byte[] bVal = ValueConverter.Boolean.toBytes(true);
        final var results = fingerprintTransformer.marshal(bVal, context(ClientDataType.BOOLEAN));
        assertArrayEquals("02:hmac:YK+q8evdVH86XycuCkXvpzCQIXdyCiMRB+ggXieNI6o=".getBytes(StandardCharsets.UTF_8), results);
    }

    @Test
    public void charIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.CHAR)));
    }

    @Test
    public void dateIsAccepted() {
        final byte[] bytes = ValueConverter.Int.toBytes(100);
        final var results = fingerprintTransformer.marshal(bytes, context(ClientDataType.DATE));
        assertArrayEquals("02:hmac:D4lN29HIikYxdDsiSqgs0+LRTmZiWBCptsJ59yKubB0=".getBytes(StandardCharsets.UTF_8), results);
    }

    @Test
    public void decimalIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.DECIMAL)));
    }

    @Test
    public void doubleIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.DOUBLE)));
    }

    @Test
    public void floatIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.FLOAT)));
    }

    @Test
    public void intIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.INT)));
    }

    @Test
    public void smallintIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.SMALLINT)));
    }

    @Test
    public void stringIsAccepted() {
        final byte[] sVal = ValueConverter.String.toBytes("12345");
        final var results = fingerprintTransformer.marshal(sVal, context(ClientDataType.STRING));
        assertArrayEquals("02:hmac:qAYEgUqI8GW2S0+1uZiYl2EWlaaPZrcHfvclC1yAQqo=".getBytes(StandardCharsets.UTF_8), results);
    }

    @Test
    public void timestampIsRejected() {
        assertThrows(C3rIllegalArgumentException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.TIMESTAMP)));
    }

    @Test
    public void varcharIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.VARCHAR)));
    }

    @Test
    public void unknownIsRejected() {
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.marshal(null, context(ClientDataType.UNKNOWN)));
    }

    @Test
    public void differentEquivalenceClassesDoNotMatchTest() {
        final CsvValue nonStringCsv = mock(CsvValue.class);
        when(nonStringCsv.getClientDataType()).thenReturn(ClientDataType.BIGINT);
        when(nonStringCsv.getBytes()).thenReturn(ValueConverter.BigInt.toBytes(100L));
        when(nonStringCsv.getBytesAs(ClientDataType.BIGINT)).thenReturn(ValueConverter.BigInt.toBytes(100L));
        final byte[] nonStringBytes = ValueConverter.getBytesForColumn(nonStringCsv, ColumnType.FINGERPRINT,
                ClientSettings.lowAssuranceMode());
        final var resultsAsBigInt = new String(fingerprintTransformer.marshal(nonStringBytes, context(ClientDataType.BIGINT)),
                StandardCharsets.UTF_8);
        assertEquals("02:hmac:XxBlrpT1ndd+Jk12ya7Finb9I9rOCzBO3ivb/inZ0eo=", resultsAsBigInt);
        final CsvValue stringCsv = mock(CsvValue.class);
        when(stringCsv.getClientDataType()).thenReturn(ClientDataType.STRING);
        when(stringCsv.getBytes()).thenReturn(ValueConverter.BigInt.toBytes(100L));
        when(stringCsv.getBytesAs(ClientDataType.STRING)).thenReturn(ValueConverter.BigInt.toBytes(100L));
        final byte[] bytes = ValueConverter.getBytesForColumn(stringCsv, ColumnType.FINGERPRINT, ClientSettings.lowAssuranceMode());
        final var resultsAsString = new String(fingerprintTransformer.marshal(bytes, context(ClientDataType.STRING)),
                StandardCharsets.UTF_8);
        assertEquals("02:hmac:1Mv2rjn0cWRjXl5nWA3dNP0hT6PXDbcDbZhayZMgNMU=", resultsAsString);
        assertNotEquals(resultsAsBigInt, resultsAsString);
    }
}
