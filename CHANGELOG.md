# Changelog

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
