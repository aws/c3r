// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.action.CsvRowUnmarshaller;
import com.amazonaws.c3r.action.ParquetRowUnmarshaller;
import com.amazonaws.c3r.action.RowUnmarshaller;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import javax.crypto.SecretKey;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.amazonaws.c3r.cli.Main.generateCommandLine;
import static com.amazonaws.c3r.encryption.keys.KeyUtil.KEY_ENV_VAR;

/**
 * Supports decrypting query results from an AWS Clean Rooms collaboration for analysis.
 */
@Slf4j
@Getter
@CommandLine.Command(name = "decrypt",
        mixinStandardHelpOptions = true,
        version = CliDescriptions.VERSION,
        descriptionHeading = "%nDescription:%n",
        description = "Decrypt data content derived from an AWS Clean Rooms collaboration.")
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

    /**
     * Return a CLI instance for decryption.
     *
     * <p>
     * Note: {@link #getApp} is the intended method for manually creating this class
     * with the appropriate CLI settings.
     */
    DecryptMode() {
    }

    /**
     * Get the decryption mode command line application with standard CLI settings.
     *
     * @return CommandLine interface for `decrypt` mode
     */
    public static CommandLine getApp() {
        return generateCommandLine(new DecryptMode());
    }

    /**
     * Get all configuration settings for the current dataset.
     *
     * @return Information needed to decrypt dataset
     */
    public DecryptConfig getConfig() {
        final SecretKey keyMaterial = KeyUtil.sharedSecretKeyFromString(System.getenv(KEY_ENV_VAR));
        return DecryptConfig.builder()
                .sourceFile(requiredArgs.getInput())
                .fileFormat(optionalArgs.fileFormat)
                .targetFile(optionalArgs.output)
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
     * @throws C3rIllegalArgumentException If collaboration identifier is missing
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

            final DecryptConfig cfg = getConfig();
            if (!optionalArgs.dryRun) {
                log.info("Decrypting data from {}.", cfg.getSourceFile());
                switch (cfg.getFileFormat()) {
                    case CSV:
                        final RowUnmarshaller<CsvValue> csvValueRowUnmarshaller = CsvRowUnmarshaller.newInstance(cfg);
                        csvValueRowUnmarshaller.unmarshal();
                        csvValueRowUnmarshaller.close();
                        break;
                    case PARQUET:
                        final RowUnmarshaller<ParquetValue> parquetRowUnmarshaller = ParquetRowUnmarshaller.newInstance(cfg);
                        parquetRowUnmarshaller.unmarshal();
                        parquetRowUnmarshaller.close();
                        break;
                    default:
                        throw new C3rIllegalArgumentException("Unrecognized file format: " + cfg.getFileFormat());
                }
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
