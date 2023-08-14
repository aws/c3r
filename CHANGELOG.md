# Changelog

## [1.2.2] - 2023-08-14

### Changed
- Bump software.amazon.awssdk:cleanrooms from 2.20.101 to 2.20.125
- Bump junit_version from 5.9.3 to 5.10.0
- Bump io.freefair.lombok from 8.1.0 to 8.2.2
- Bump io.freefair.javadocs from 8.1.0 to 8.2.2
- Bump com.github.spotbugs from 5.0.14 to 5.1.2
- Bump trufflesecurity/trufflehog from 3.43.0 to 3.48.0

## [1.2.1] - 2023-07-10

### Changed
- Decryption no longer normalizes column headers (#243)
- Bump Spark dependencies from 3.4.0 to 3.4.1
- Bump software.amazon.awssdk:cleanrooms from 2.20.82 to 2.20.101
- Bump Hadoop dependencies from 3.3.5 to 3.3.6
- Bump freefair plugins from 8.0.1 to 8.1.0
- Bump trufflesecurity/trufflehog from 3.39.0 to 3.43.0

### Deprecated
+ Deprecated constructors for `ParquetRowReader` and `ParquetSchema`, see class builders instead
+ Deprecated `ColumnHeader.getColumnHeaderFromIndex` in favor of `ColumnHeader.of(int)`

## [1.2.0] - 2023-06-08

### Added
- Initial C3R client for Apache Spark `c3r-cli-spark` (#223)
- Customize user agent, shared version constants (#219)

### Changed
- Use Clean Rooms header limits, deprecate Glue limit (#209)
- Update input/output column count limits (#190)
- Bump software.amazon.awssdk:cleanrooms from 2.20.56 to 2.20.82
- Bump org.xerial:sqlite-jdbc from 3.41.2.1 to 3.42.0.0 (#205)
- Bump org.apache.parquet:parquet-hadoop from 1.13.0 to 1.13.1 (#201)
- Bump info.picocli:picocli from 4.7.1 to 4.7.4 (#218)
- Bump trufflesecurity/trufflehog from 3.33.0 to 3.39.0 (#216)

### Deprecated
+ `Limits.GLUE_VALID_HEADER_REGEXP`, see `Limits.AWS_CLEAN_ROOMS_HEADER_REGEXP`
+ `Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH`, see `Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH`

## [1.1.3] - 2023-05-01

### Changed
- Deprecated Config class' transformer field (#157)
- Bump software.amazon.awssdk:cleanrooms from 2.20.37 to 2.20.56
- Bump org.apache.parquet:parquet-hadoop from 1.12.3 to 1.13.0
- Bump org.junit.jupiter:junit-jupiter-api from 5.9.2 to 5.9.3
- Bump org.junit.jupiter:junit-jupiter-params from 5.9.2 to 5.9.3
- Bump org.junit.jupiter:junit-jupiter-engine from 5.9.2 to 5.9.3
- Bump info.picocli:picocli from 4.7.1 to 4.7.3
- Bump trufflesecurity/trufflehog 3.31.2 to 3.33.0

## [1.1.2] - 2023-04-04

### Changed
- Bump software.amazon.awssdk:cleanrooms from 2.20.28 to 2.20.37
- Bump org.xerial:sqlite-jdbc from 3.41.0.1 to 3.41.2.1
- Bump org.apache.hadoop:hadoop-mapreduce-client-core from 3.3.4 to 3.3.5
- Bump org.apache.hadoop:hadoop-common from 3.3.4 to 3.3.5
- Bump io.github.hakky54:logcaptor from 2.8.0 to 2.9.0
- Bump com.github.spotbugs from 5.0.13 to 5.0.14
- Bump trufflesecurity/trufflehog from 3.29.1 to 3.31.2

## [1.1.1] - 2023-03-21

### Changed
- ColumnInsight moved from SDK internal to SDK config dir (#114)
- Config.initTransformers moved to Transformer class and made publicly available (#114)
- JSON utilities and AWS Clean Rooms DAO moved from CLI to SDK (#112)
- Bump trufflesecurity/trufflehog from 3.28.7 to 3.29.1
- Bump Gradle from 7.5.1 to 8.0.1
- Bump software.amazon.awssdk:cleanrooms from 2.20.17 to 2.20.28
- Bump org.xerial:sqlite-jdbc from 3.41.0.0 to 3.41.0.1
- Bump org.mockito:mockito-inline from 5.1.1 to 5.2.0
- Bump org.mockito:mockito-core from 5.1.1 to 5.2.0
- Bump io.freefair.lombok from 6.6.3 to 8.0.1
- Bump io.freefair.javadocs from 6.6.3 to 8.0.1
- Bump com.github.johnrengelman.shadow from 7.1.2 to 8.1.1

### Added
- Add CSV column count validation (#97)
- Add Spark example (#117)

### Fixed
- POM file generated for Maven release now includes dependencies (#124)

## [1.1.0] - 2023-03-06

### Changed
- Update maven-related gradle configurations (#27)
- Link to the shared secret generation documentation (#32)
- Improved max padding description during schema generation (#48)
- Improved AWS SDK exception wrapping (#49)
- Move example files to an examples package (#55)
- Drop ignores from checkstyle rule for enforcing copyright headers (#71)
- Bump org.apache.logging.log4j:log4j-slf4j-impl from 2.19.0 to 2.20.0
- Bump org.apache.logging.log4j:log4j-api from 2.19.0 to 2.20.0
- Bump org.apache.logging.log4j:log4j-core from 2.19.0 to 2.20.0
- Bump io.freefair.javadocs from 6.6.1 to 6.6.3
- Bump io.github.hakky54:logcaptor from 2.7.10 to 2.8.0
- Bump org.mockito:mockito-core from 4.8.0 to 5.1.1
- Bump org.xerial:sqlite-jdbc from 3.39.3.0 to 3.41.0.0
- Bump picocli from 4.6.3 to 4.7.1
- Bump mockito-inline from 4.9.0 to 5.1.1
- Bump junit-jupiter-api from 5.9.1 to 5.9.2
- Bump gson from 2.9.1 to 2.10.1
- Bump io.freefair.lombok from 6.6 to 6.6.3
- Bump junit-jupiter-engine from 5.9.1 to 5.9.2
- Bump software.amazon.awssdk:cleanrooms from 2.19.16 to 2.20.17
- Bump trufflesecurity/trufflehog from 3.23.0 to 3.28.7
- Bump fkirc/skip-duplicate-actions from 5.2.0 to 5.3.0

### Added
- Add optional --id CLI flag for assisted schema generation (#28)
- Add optional --profile and --region CLI flags (#69)

### Fixed
- Fix CLI arg parsing bugs (#70)
- Warn on custom null when no cleartext targets (#46)

## [1.0.0] - 2023-01-12

_First release._
