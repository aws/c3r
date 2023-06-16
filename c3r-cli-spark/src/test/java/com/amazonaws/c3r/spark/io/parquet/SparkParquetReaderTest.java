// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.parquet;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.spark.config.SparkEncryptConfig;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.Iterable;
import scala.collection.immutable.Seq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS;
import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS_NO_NORMALIZATION;
import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.EXAMPLE_SALT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparkParquetReaderTest {

    private static SparkSession session;

    private static TableSchema schema;

    private static SparkEncryptConfig config;

    /**
     * Initial setup done only once because the data is immutable and starting Spark sessions each time is expensive.
     *
     * @throws IOException if Schema can't be read.
     */
    @BeforeAll
    public static void setup() throws IOException {
        schema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"), TableSchema.class);
        session = SparkSessionTestUtility.initSparkSession();
        config = SparkEncryptConfig.builder()
                .source("../samples/parquet/data_sample.parquet")
                .targetDir(FileTestUtility.createTempDir().resolve("output").toString())
                .overwrite(true)
                .secretKey(KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR)))
                .salt(EXAMPLE_SALT.toString())
                .tableSchema(schema)
                .settings(ClientSettings.lowAssuranceMode())
                .build();
    }

    @Test
    public void readInputColumnsTest() {
        final Dataset<Row> dataset = SparkParquetReader.readInput(session, config.getSourceFile());
        final List<String> columns = Arrays.stream(dataset.columns())
                .sorted()
                .collect(Collectors.toList());
        assertEquals(
                DATA_SAMPLE_HEADERS.stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()),
                columns);
    }

    @Test
    public void readInputColumnsNoNormalizationTest() {
        final Dataset<Row> dataset = SparkParquetReader.readInput(session, config.getSourceFile(), /* skipHeaderNormalization */ true);
        final List<String> columns = Arrays.stream(dataset.columns())
                .sorted()
                .collect(Collectors.toList());
        assertEquals(
                DATA_SAMPLE_HEADERS_NO_NORMALIZATION.stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()),
                columns);
    }

    @Test
    public void readInputDirectoryTest() throws IOException {
        final Path tempDir = FileTestUtility.createTempDir();
        final Path copiedFile = tempDir.resolve("copied.parquet");
        Files.copy(Path.of("../samples/parquet/data_sample.parquet"), copiedFile);
        final Dataset<Row> dataset = SparkParquetReader.readInput(session, tempDir.toString());
        final List<String> columns = Arrays.stream(dataset.columns())
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.toList());
        final List<String> expectedColumns = schema.getColumns().stream()
                .map(columnSchema -> columnSchema.getSourceHeader().toString())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        assertEquals(expectedColumns.size(), columns.size());
        assertTrue(expectedColumns.containsAll(columns));
    }

    @Test
    public void maxColumnCountTest() {
        final Dataset<Row> dataset = mock(Dataset.class);
        when(dataset.columns()).thenReturn(new String[SparkParquetReader.MAX_COLUMN_COUNT + 1]);
        when(dataset.count()).thenReturn(0L); // in range row size
        assertThrows(C3rRuntimeException.class, () -> SparkParquetReader.validate(dataset));
    }

    @Test
    public void maxRowCountTest() {
        final Dataset<Row> dataset = mock(Dataset.class);
        when(dataset.columns()).thenReturn(new String[0]); // in range column size
        when(dataset.count()).thenReturn(Limits.ROW_COUNT_MAX + 1L);
        assertThrows(C3rRuntimeException.class, () -> SparkParquetReader.validate(dataset));
    }

    @Test
    public void maliciousColumnHeaderTest() throws IOException {
        final StructField maliciousColumn = DataTypes.createStructField("; DROP ALL TABLES;", DataTypes.StringType, true);
        final StructType maliciousSchema = DataTypes.createStructType(new StructField[]{maliciousColumn});
        final ArrayList<Row> data = new ArrayList<>();
        data.add(Row.fromSeq(Seq.from(Iterable.single("value"))));
        final Dataset<Row> maliciousDataset = session.createDataFrame(data, maliciousSchema);
        final Path tempDir = FileTestUtility.createTempDir();
        SparkParquetWriter.writeOutput(maliciousDataset, tempDir.toString());
        final Dataset<Row> dataset = SparkParquetReader.readInput(session, tempDir.toString());

        /*
         Assert the malicious header is like any other.

         While the standard Spark Parquet reader will allow special chars, since a ColumnHeader will not, we can assume
         any fields like this will be dropped later before any further parsing.
         */
        assertEquals(maliciousColumn.name(), dataset.columns()[0]);

        // Assert values still exist
        assertFalse(dataset.isEmpty());
    }
}
