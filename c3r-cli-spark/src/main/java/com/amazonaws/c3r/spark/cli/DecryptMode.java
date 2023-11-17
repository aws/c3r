// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.config.ParquetConfig;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.spark.action.SparkUnmarshaller;
import com.amazonaws.c3r.spark.config.SparkDecryptConfig;
import com.amazonaws.c3r.spark.io.csv.SparkCsvReader;
import com.amazonaws.c3r.spark.io.csv.SparkCsvWriter;
import com.amazonaws.c3r.spark.io.parquet.SparkParquetReader;
import com.amazonaws.c3r.spark.io.parquet.SparkParquetWriter;
import com.amazonaws.c3r.spark.utils.SparkSessionUtil;
import com.amazonaws.c3r.utils.C3rSdkProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import javax.crypto.SecretKey;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.amazonaws.c3r.encryption.keys.KeyUtil.KEY_ENV_VAR;
import static com.amazonaws.c3r.spark.cli.Main.generateCommandLine;

/**
 * Supports decrypting query results from an AWS Clean Rooms collaboration for analysis.
 */
@Slf4j
@Getter
@CommandLine.Command(name = "decrypt",
        mixinStandardHelpOptions = true,
        version = C3rSdkProperties.VERSION,
        descriptionHeading = "%nDescription:%n",
        description = "Decrypt data content derived from an AWS Clean Rooms collaboration as an Apache Spark job.")
public class DecryptMode implements Callable<Integer> {

    /**
     * Required command line arguments.
     */
    @Getter
    static class RequiredArgs {

        /**
         * {@value CliDescriptions#INPUT_DESCRIPTION_CRYPTO}.
         */
        @picocli.CommandLine.Parameters(description = CliDescriptions.INPUT_DESCRIPTION_CRYPTO,
                paramLabel = "<input>")
        private String input = null;

        /**
         * {@value CliDescriptions#ID_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--id"},
                description = CliDescriptions.ID_DESCRIPTION,
                paramLabel = "<value>",
                required = true)
        private UUID id = null;
    }

    /**
     * Required values as specified by the user.
     */
    @CommandLine.ArgGroup(multiplicity = "1", exclusive = false, heading = "%nRequired parameters:%n")
    private RequiredArgs requiredArgs = new RequiredArgs();

    /**
     * Optional command line arguments.
     */
    @Getter
    private static class OptionalArgs {
        /**
         * {@value CliDescriptions#FILE_FORMAT_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--fileFormat", "-e"},
                description = CliDescriptions.FILE_FORMAT_DESCRIPTION,
                paramLabel = "<format>")
        private FileFormat fileFormat = null;

        /**
         * {@value CliDescriptions#OUTPUT_DESCRIPTION_CRYPTO}.
         */
        @CommandLine.Option(names = {"--output", "-o"},
                description = CliDescriptions.OUTPUT_DESCRIPTION_CRYPTO,
                paramLabel = "<file>")
        private String output = null;

        /**
         * {@value CliDescriptions#OVERWRITE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--overwrite", "-f"},
                description = CliDescriptions.OVERWRITE_DESCRIPTION)
        private boolean overwrite = false;

        /**
         * {@value CliDescriptions#DECRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--csvInputNULLValue", "-r"},
                description = CliDescriptions.DECRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION,
                paramLabel = "<value>")
        private String csvInputNullValue = null;

        /**
         * {@value CliDescriptions#DECRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--csvOutputNULLValue", "-w"},
                description = CliDescriptions.DECRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION,
                paramLabel = "<value>")
        private String csvOutputNullValue = null;

        /**
         * {@value CliDescriptions#FAIL_ON_FINGERPRINT_COLUMNS_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--failOnFingerprintColumns", "--fof"},
                description = CliDescriptions.FAIL_ON_FINGERPRINT_COLUMNS_DESCRIPTION)
        private boolean failOnFingerprintColumns = false;

        /**
         * {@value CliDescriptions#DRY_RUN_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--dryRun", "-n"},
                description = CliDescriptions.DRY_RUN_DESCRIPTION)
        private boolean dryRun = false;

        /**
         * {@value CliDescriptions#ENABLE_STACKTRACE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--enableStackTraces", "-v"},
                description = CliDescriptions.ENABLE_STACKTRACE_DESCRIPTION)
        private boolean enableStackTraces = false;
    }

    /**
     * Optional values as specified by the user.
     */
    @CommandLine.ArgGroup(exclusive = false, heading = "%nOptional parameters:%n")
    private OptionalArgs optionalArgs = new OptionalArgs();

    /** SparkSession for orchestration. */
    private final SparkSession sparkSession;

    /**
     * Return a CLI instance for decryption.
     *
     * <p>
     * Note: {@link #getApp} is the intended method for manually creating this class
     * with the appropriate CLI settings.
     */
    DecryptMode() {
        this.sparkSession = SparkSessionUtil.initSparkSession();
    }

    /**
     * Return a CLI instance for decryption with a custom SparkSession.
     *
     * <p>
     * Note: {@link #getApp} is the intended method for manually creating this class
     * with the appropriate CLI settings.
     *
     * @param sparkSession Custom SparkSession to use for orchestration
     */
    DecryptMode(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * Get the decryption mode command line application with standard CLI settings.
     *
     * @param sparkSession Custom SparkSession to use for orchestration
     * @return CommandLine interface for `decrypt` mode
     */
    public static CommandLine getApp(final SparkSession sparkSession) {
        return generateCommandLine(new DecryptMode(sparkSession));
    }

    /**
     * Get all configuration settings for the current dataset.
     *
     * @return Information needed to decrypt dataset
     */
    public SparkDecryptConfig getConfig() {
        final SecretKey keyMaterial = KeyUtil.sharedSecretKeyFromString(System.getenv(KEY_ENV_VAR));
        return SparkDecryptConfig.builder()
                .source(requiredArgs.getInput())
                .fileFormat(optionalArgs.fileFormat)
                .targetDir(optionalArgs.output)
                .overwrite(optionalArgs.overwrite)
                .csvInputNullValue(optionalArgs.csvInputNullValue)
                .csvOutputNullValue(optionalArgs.csvOutputNullValue)
                .secretKey(keyMaterial)
                .salt(requiredArgs.getId().toString())
                .failOnFingerprintColumns(optionalArgs.failOnFingerprintColumns)
                .build();
    }

    /**
     * Ensure requirements are met to run.
     *
     * @throws C3rIllegalArgumentException If user input is invalid
     */
    private void validate() {
        if (requiredArgs.getId() == null || requiredArgs.getId().toString().isBlank()) {
            throw new C3rIllegalArgumentException("Specified collaboration identifier is blank.");
        }
    }

    /**
     * Execute the decryption as specified on the command line.
     *
     * @return {@value Main#SUCCESS} if no errors encountered else {@value Main#FAILURE}
     */
    @Override
    public Integer call() {
        try {
            validate();

            final SparkDecryptConfig cfg = getConfig();
            if (!optionalArgs.dryRun) {
                log.info("Decrypting data from {}.", cfg.getSourceFile());
                switch (cfg.getFileFormat()) {
                    case CSV:
                        final Dataset<Row> csvDataset = SparkCsvReader.readInput(sparkSession,
                                cfg.getSourceFile(),
                                cfg.getCsvInputNullValue(),
                                /* externalHeaders */ null,
                                /* skipHeaderNormalization */ true);
                        final Dataset<Row> unmarshalledCsvDataset = SparkUnmarshaller.decrypt(csvDataset, cfg);
                        SparkCsvWriter.writeOutput(unmarshalledCsvDataset, cfg.getTargetFile(), cfg.getCsvOutputNullValue());
                        break;
                    case PARQUET:
                        final Dataset<Row> parquetDataset = SparkParquetReader.readInput(
                                sparkSession,
                                cfg.getSourceFile(),
                                /* skipHeaderNormalization */ true,
                                ParquetConfig.DEFAULT);
                        final Dataset<Row> unmarshalledParquetDataset = SparkUnmarshaller.decrypt(parquetDataset, cfg);
                        SparkParquetWriter.writeOutput(unmarshalledParquetDataset, cfg.getTargetFile());
                        break;
                    default:
                        throw new C3rIllegalArgumentException("Unrecognized file format: " + cfg.getFileFormat());
                }
                SparkSessionUtil.closeSparkSession(sparkSession);
                log.info("Decrypted data saved in {}.", cfg.getTargetFile());
            } else {
                log.info("Dry run: No data will be decrypted from {}.", cfg.getSourceFile());
            }
        } catch (Exception e) {
            Main.handleException(e, optionalArgs.enableStackTraces);
            return Main.FAILURE;
        }

        return Main.SUCCESS;
    }
}
