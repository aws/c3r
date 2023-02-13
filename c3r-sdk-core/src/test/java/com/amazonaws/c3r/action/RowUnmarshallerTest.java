// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.CleartextTransformer;
import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;
import com.amazonaws.c3r.Transformer;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_MARSHALLED_DATA_SAMPLE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowUnmarshallerTest {
    private String output;

    private Map<ColumnType, Transformer> transformers;

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.resolve("output.csv").toString();
        final Encryptor encryptor = Encryptor.getInstance(new SymmetricStaticProvider(TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getKey(),
                TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getSalt().getBytes(StandardCharsets.UTF_8)));
        transformers = new HashMap<>();
        transformers.put(ColumnType.CLEARTEXT, new CleartextTransformer());
        transformers.put(ColumnType.FINGERPRINT, new FingerprintTransformer(TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getKey(),
                TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getSalt().getBytes(StandardCharsets.UTF_8), null, false));
        transformers.put(ColumnType.SEALED, new SealedTransformer(encryptor, null));
    }

    @Test
    public void unmarshalTransformerFailureTest() {
        final SealedTransformer badCleartextTransformer = mock(SealedTransformer.class);
        when(badCleartextTransformer.unmarshal(any())).thenThrow(new C3rRuntimeException("error"));
        transformers.put(ColumnType.CLEARTEXT, badCleartextTransformer);

        final RowUnmarshaller<CsvValue> unmarshaller = CsvRowUnmarshaller.builder()
                .sourceFile(TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getInput())
                .targetFile(output)
                .transformers(transformers)
                .build();
        assertThrows(C3rRuntimeException.class, unmarshaller::unmarshal);
    }

    @Test
    public void endToEndUnmarshalTest() {
        final RowUnmarshaller<CsvValue> unmarshaller = CsvRowUnmarshaller.builder()
                .sourceFile(TEST_CONFIG_MARSHALLED_DATA_SAMPLE.getInput())
                .targetFile(output)
                .transformers(transformers)
                .build();
        unmarshaller.unmarshal();
        final String file = FileUtil.readBytes(Path.of(output).toAbsolutePath().toString());
        assertFalse(file.isBlank());
        unmarshaller.close();
    }
}
