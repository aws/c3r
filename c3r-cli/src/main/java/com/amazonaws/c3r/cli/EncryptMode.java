// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.action.CsvRowMarshaller;
import com.amazonaws.c3r.action.ParquetRowMarshaller;
import com.amazonaws.c3r.action.RowMarshaller;
import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.utils.FileUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.amazonaws.c3r.cli.Main.generateCommandLine;

/**
 * Supports cryptographic computations on data in preparation for upload to an AWS Clean Rooms collaboration.
 */
@Slf4j
@Getter
@CommandLine.Command(name = "encrypt",
        mixinStandardHelpOptions = true,
        version = CliDescriptions.VERSION,
        descriptionHeading = "%nDescription:%n",
        description = "Encrypt data content for use in an AWS Clean Rooms collaboration.")
public class EncryptMode implements Callable<Integer> {

    /**
     * Required command line arguments.
     */
    @Getter
    static class RequiredArgs {
        /**
         * {@value CliDescriptions#INPUT_DESCRIPTION_CRYPTO}.
         */
        @CommandLine.Parameters(description = CliDescriptions.INPUT_DESCRIPTION_CRYPTO,
                paramLabel = "<input>")
        private String input = null;

        /**
         * {@value CliDescriptions#SCHEMA_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--schema", "-s"},
                description = CliDescriptions.SCHEMA_DESCRIPTION,
                required = true,
                paramLabel = "<file>")
        private String schema = null;

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
    @CommandLine.ArgGroup(validate = false, heading = "%nRequired parameters:%n")
    private RequiredArgs requiredArgs = new RequiredArgs();

    /**
     * Optional command line arguments.
     */
    @Getter
    static class OptionalArgs {
        /**
         * {@value CliDescriptions#AWS_PROFILE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--profile", "-l"},
                description = CliDescriptions.AWS_PROFILE_DESCRIPTION)
        private String profile = null;

        /**
         * {@value CliDescriptions#AWS_REGION_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--region", "-g"},
                description = CliDescriptions.AWS_REGION_DESCRIPTION)
        private String region = null;

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
         * {@value CliDescriptions#DRY_RUN_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--dryRun", "-n"},
                description = CliDescriptions.DRY_RUN_DESCRIPTION)
        private boolean dryRun = false;

        /**
         * {@value CliDescriptions#ENCRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--csvInputNULLValue", "-r"},
                description = CliDescriptions.ENCRYPT_CSV_INPUT_NULL_VALUE_DESCRIPTION,
                paramLabel = "<value>")
        private String csvInputNullValue = null;

        /**
         * {@value CliDescriptions#ENCRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--csvOutputNULLValue", "-w"},
                description = CliDescriptions.ENCRYPT_CSV_OUTPUT_NULL_VALUE_DESCRIPTION,
                paramLabel = "<value>")
        private String csvOutputNullValue = null;

        /**
         * {@value CliDescriptions#TEMP_DIR_DESCRIPTION}.
         */
        @CommandLine.Option(names = {"--tempDir", "-d"},
                description = CliDescriptions.TEMP_DIR_DESCRIPTION,
                paramLabel = "<dir>")
        private String tempDir = FileUtil.TEMP_DIR;

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
    @CommandLine.ArgGroup(validate = false, heading = "%nOptional parameters:%n")
    private OptionalArgs optionalArgs = new OptionalArgs();

    /** DAO for interacting with AWS Clean Rooms. */
    private final CleanRoomsDao cleanRoomsDao;

    /**
     * Return a default CLI instance for encryption.
     *
     * <p>
     * Note: {@link #getApp} is the intended method for manually creating this class
     * with the appropriate CLI settings.
     */
    EncryptMode() {
        this.cleanRoomsDao = CleanRoomsDao.builder().build();
    }

    /**
     * Return a CLI instance for an encryption pass with a custom {@link CleanRoomsDao}.
     *
     * <p>
     * Note: {@link #getApp} is the intended method for manually creating this class
     * with the appropriate CLI settings.
     *
     * @param cleanRoomsDao Custom {@link CleanRoomsDao} to use for Clean Rooms API calls
     */
    EncryptMode(final CleanRoomsDao cleanRoomsDao) {
        this.cleanRoomsDao = cleanRoomsDao;
    }

    /**
     * Get the encrypt mode command line application with a custom {@link CleanRoomsDao}.
     *
     * @param cleanRoomsDao Custom {@link CleanRoomsDao} to use for Clean Rooms API calls
     * @return CommandLine interface for `encrypt` with customized AWS Clean Rooms access and standard CLI settings
     */
    public static CommandLine getApp(final CleanRoomsDao cleanRoomsDao) {
        return generateCommandLine(new EncryptMode(cleanRoomsDao));
    }

    /**
     * Get the settings from AWS Clean Rooms for this collaboration.
     *
     * @return Cryptographic computing rules for collaboration
     */
    public ClientSettings getClientSettings() {
        final var region = optionalArgs.region == null ? null : Region.of(optionalArgs.region);
        final var dao = cleanRoomsDao != null ? cleanRoomsDao : CleanRoomsDao.builder().build();
        return dao.withProfile(optionalArgs.profile).withRegion(region)
                .getCollaborationDataEncryptionMetadata(requiredArgs.id.toString());
    }

    /**
     * All the configuration information needed for encrypting data.
     *
     * @return All cryptographic settings and information on data processing
     * @throws C3rRuntimeException         If the schema file can't be parsed
     * @throws C3rIllegalArgumentException If the schema file is empty
     */
    public EncryptConfig getConfig() {
        final String tempDir = (optionalArgs.tempDir != null) ? optionalArgs.tempDir : FileUtil.TEMP_DIR;
        final SecretKey keyMaterial = KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR));
        final TableSchema tableSchema;
        try {
            tableSchema = GsonUtil.fromJson(FileUtil.readBytes(requiredArgs.getSchema()), TableSchema.class);
        } catch (Exception e) {
            throw new C3rRuntimeException("Failed to parse the table schema file: " + requiredArgs.getSchema()
                    + ". Please see the stack trace for where the parsing failed.", e);
        }
        if (tableSchema == null) {
            throw new C3rIllegalArgumentException("The table schema file was empty: " + requiredArgs.getSchema());
        }

        return EncryptConfig.builder()
                .sourceFile(requiredArgs.getInput())
                .fileFormat(optionalArgs.fileFormat)
                .targetFile(optionalArgs.output)
                .tempDir(tempDir)
                .overwrite(optionalArgs.overwrite)
                .csvInputNullValue(optionalArgs.csvInputNullValue)
                .csvOutputNullValue(optionalArgs.csvOutputNullValue)
                .secretKey(keyMaterial)
                .salt(requiredArgs.getId().toString())
                .settings(getClientSettings())
                .tableSchema(tableSchema)
                .build();
    }

    /**
     * Ensure required settings exist.
     *
     * @throws C3rIllegalArgumentException If user input is invalid
     */
    private void validate() {
        FileUtil.verifyReadableFile(requiredArgs.getSchema());
        if (requiredArgs.getId() == null || requiredArgs.getId().toString().isBlank()) {
            throw new C3rIllegalArgumentException("Specified collaboration identifier is blank.");
        }
    }

    /**
     * Log information about how the data is being encrypted.
     *
     * @param columnSchemas Description of how input data should be transformed during the encryption process
     */
    void printColumCategoryInfo(final List<ColumnSchema> columnSchemas) {
        if (columnSchemas.isEmpty()) {
            return;
        }
        log.info("{} {} column{} being generated:",
                columnSchemas.size(),
                columnSchemas.get(0).getType(),
                columnSchemas.size() > 1 ? "s" : "");
        for (var c : columnSchemas) {
            log.info("  * " + c.getTargetHeader());
        }
    }

    /**
     * Print summary information about what will be in the encrypted output.
     *
     * @param tableSchema How data will be transformed during encryption
     */
    private void printColumnTransformInfo(final TableSchema tableSchema) {
        final var sealedColumns = new ArrayList<ColumnSchema>();
        final var fingerprintColumns = new ArrayList<ColumnSchema>();
        final var cleartextColumns = new ArrayList<ColumnSchema>();

        for (var c : tableSchema.getColumns()) {
            switch (c.getType()) {
                case SEALED:
                    sealedColumns.add(c);
                    break;
                case FINGERPRINT:
                    fingerprintColumns.add(c);
                    break;
                default:
                    cleartextColumns.add(c);
                    break;

            }
        }
        printColumCategoryInfo(sealedColumns);
        printColumCategoryInfo(fingerprintColumns);
        printColumCategoryInfo(cleartextColumns);
    }

    /**
     * Encrypt data for upload to an AWS Clean Rooms.
     *
     * @return {@link Main#SUCCESS} if no errors, else {@link Main#FAILURE}
     */
    @Override
    public Integer call() {
        try {
            validate();

            final EncryptConfig cfg = getConfig();

            printColumnTransformInfo(cfg.getTableSchema());
            if (!optionalArgs.dryRun) {
                log.info("Encrypting data from {}.", cfg.getSourceFile());
                switch (cfg.getFileFormat()) {
                    case CSV:
                        final RowMarshaller<CsvValue> csvRowMarshaller = CsvRowMarshaller.newInstance(cfg);
                        csvRowMarshaller.marshal();
                        csvRowMarshaller.close();
                        break;
                    case PARQUET:
                        final RowMarshaller<ParquetValue> parquetRowMarshaller = ParquetRowMarshaller.newInstance(cfg);
                        parquetRowMarshaller.marshal();
                        parquetRowMarshaller.close();
                        break;
                    default:
                        throw new C3rIllegalArgumentException("Unrecognized file format: " + cfg.getFileFormat());
                }
                log.info("Encrypted data was saved to {}.", cfg.getTargetFile());
            } else {
                log.info("Dry run: No data will be encrypted from {}.", cfg.getSourceFile());
            }
        } catch (Exception e) {
            Main.handleException(e, optionalArgs.enableStackTraces);
            return Main.FAILURE;
        }

        return Main.SUCCESS;
    }
}