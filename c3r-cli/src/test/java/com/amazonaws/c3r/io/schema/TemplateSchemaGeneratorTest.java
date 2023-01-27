// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TemplateSchemaGeneratorTest {
    private Path tempSchema;

    @BeforeEach
    public void setup() throws IOException {
        final Path tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        tempSchema = tempDir.resolve("schema.json");
        tempSchema.toFile().deleteOnExit();
    }

    @Test
    public void validateErrorWithMismatchedColumnCounts() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                TemplateSchemaGenerator.builder()
                        .sourceHeaders(List.of(new ColumnHeader("Column 1")))
                        .sourceColumnTypes(List.of())
                        .targetJsonFile(tempSchema.toAbsolutePath().toString())
                        .build());
    }

    @Test
    public void testTemplateWithSourceHeadersNoSettingsGeneration() throws IOException {
        final var expectedContent = String.join("\n",
                "{",
                "  \"headerRow\": true,",
                "  \"columns\": [",
                "    {",
                "      \"sourceHeader\": \"header1\",",
                "      \"targetHeader\": \"header1\",",
                "      \"type\": \"[sealed|fingerprint|cleartext]\",",
                "      \"pad\": {",
                "        \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "        \"type\": \"[none|fixed|max]\",",
                "        \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "      }",
                "    },",
                "    {",
                "      \"sourceHeader\": \"header2\",",
                "      \"targetHeader\": \"header2\",",
                "      \"type\": \"cleartext\"",
                "    }",
                "  ]",
                "}"
        );

        final var headers = List.of(
                new ColumnHeader("header1"),
                new ColumnHeader("header2")
        );
        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);
        final var generator = TemplateSchemaGenerator.builder()
                .sourceHeaders(headers)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .build();
        generator.run();
        final String content = Files.readString(tempSchema, StandardCharsets.UTF_8);

        assertEquals(expectedContent, content);
    }

    @Test
    public void testTemplateWithoutSourceHeadersNoSettingsGeneration() throws IOException {
        final String expectedPositionalSchemaOutput = String.join("\n",
                "{",
                "  \"headerRow\": false,",
                "  \"columns\": [",
                "    [",
                "      {",
                "        \"targetHeader\": \"column 1\",",
                "        \"type\": \"[sealed|fingerprint|cleartext]\",",
                "        \"pad\": {",
                "          \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "          \"type\": \"[none|fixed|max]\",",
                "          \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "        }",
                "      }",
                "    ],",
                "    [",
                "      {",
                "        \"targetHeader\": \"column 2\",",
                "        \"type\": \"cleartext\"",
                "      }",
                "    ]",
                "  ]",
                "}");

        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);

        TemplateSchemaGenerator.builder()
                .sourceHeaders(null)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .build()
                .run();
        final String content = Files.readString(tempSchema);

        assertEquals(expectedPositionalSchemaOutput, content);
    }

    @Test
    public void testTemplateWithSourceHeadersPermissiveSettingsGeneration() throws IOException {
        final var expectedContent = String.join("\n",
                "{",
                "  \"headerRow\": true,",
                "  \"columns\": [",
                "    {",
                "      \"sourceHeader\": \"header1\",",
                "      \"targetHeader\": \"header1\",",
                "      \"type\": \"[sealed|fingerprint|cleartext]\",",
                "      \"pad\": {",
                "        \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "        \"type\": \"[none|fixed|max]\",",
                "        \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "      }",
                "    },",
                "    {",
                "      \"sourceHeader\": \"header2\",",
                "      \"targetHeader\": \"header2\",",
                "      \"type\": \"cleartext\"",
                "    }",
                "  ]",
                "}"
        );

        final var headers = List.of(
                new ColumnHeader("header1"),
                new ColumnHeader("header2")
        );
        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);
        final var generator = TemplateSchemaGenerator.builder()
                .sourceHeaders(headers)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .clientSettings(ClientSettings.lowAssuranceMode())
                .build();
        generator.run();
        final String content = Files.readString(tempSchema, StandardCharsets.UTF_8);

        assertEquals(expectedContent, content);
    }

    @Test
    public void testTemplateWithoutSourceHeadersPermissiveSettingsGeneration() throws IOException {
        final String expectedPositionalSchemaOutput = String.join("\n",
                "{",
                "  \"headerRow\": false,",
                "  \"columns\": [",
                "    [",
                "      {",
                "        \"targetHeader\": \"column 1\",",
                "        \"type\": \"[sealed|fingerprint|cleartext]\",",
                "        \"pad\": {",
                "          \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "          \"type\": \"[none|fixed|max]\",",
                "          \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "        }",
                "      }",
                "    ],",
                "    [",
                "      {",
                "        \"targetHeader\": \"column 2\",",
                "        \"type\": \"cleartext\"",
                "      }",
                "    ]",
                "  ]",
                "}");

        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);

        TemplateSchemaGenerator.builder()
                .sourceHeaders(null)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .clientSettings(ClientSettings.lowAssuranceMode())
                .build()
                .run();
        final String content = Files.readString(tempSchema);

        assertEquals(expectedPositionalSchemaOutput, content);
    }

    @Test
    public void testTemplateWithSourceHeadersRestrictiveSettingsGeneration() throws IOException {
        final var expectedContent = String.join("\n",
                "{",
                "  \"headerRow\": true,",
                "  \"columns\": [",
                "    {",
                "      \"sourceHeader\": \"header1\",",
                "      \"targetHeader\": \"header1\",",
                "      \"type\": \"[sealed|fingerprint]\",",
                "      \"pad\": {",
                "        \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "        \"type\": \"[none|fixed|max]\",",
                "        \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "      }",
                "    }",
                "  ]",
                "}"
        );

        final var headers = List.of(
                new ColumnHeader("header1"),
                new ColumnHeader("header2")
        );
        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);
        final var generator = TemplateSchemaGenerator.builder()
                .sourceHeaders(headers)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .clientSettings(ClientSettings.highAssuranceMode())
                .build();
        generator.run();
        final String content = Files.readString(tempSchema, StandardCharsets.UTF_8);

        assertEquals(expectedContent, content);
    }

    @Test
    public void testTemplateWithoutSourceHeadersRestrictiveSettingsGeneration() throws IOException {
        final String expectedPositionalSchemaOutput = String.join("\n",
                "{",
                "  \"headerRow\": false,",
                "  \"columns\": [",
                "    [",
                "      {",
                "        \"targetHeader\": \"column 1\",",
                "        \"type\": \"[sealed|fingerprint]\",",
                "        \"pad\": {",
                "          \"COMMENT\": \"omit this pad entry unless column type is sealed\",",
                "          \"type\": \"[none|fixed|max]\",",
                "          \"length\": \"omit length property for type none, otherwise specify value in [0, 10000]\"",
                "        }",
                "      }",
                "    ],",
                "    []",
                "  ]",
                "}");

        final List<ClientDataType> types = List.of(ClientDataType.STRING, ClientDataType.UNKNOWN);

        TemplateSchemaGenerator.builder()
                .sourceHeaders(null)
                .sourceColumnTypes(types)
                .targetJsonFile(tempSchema.toAbsolutePath().toString())
                .clientSettings(ClientSettings.highAssuranceMode())
                .build()
                .run();
        final String content = Files.readString(tempSchema);

        assertEquals(expectedPositionalSchemaOutput, content);
    }
}
