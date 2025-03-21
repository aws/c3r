# Cryptographic Computing for Clean Rooms (C3R)

The Cryptographic Computing for Clean Rooms (C3R) encryption client and software development kit (SDK) provide client-side tooling which allows users to participate in AWS Clean Rooms collaborations leveraging cryptographic computing by pre- and post-processing data.

The [AWS Clean Rooms User Guide](https://docs.aws.amazon.com/clean-rooms/latest/userguide/index.html) contains detailed information regarding how to use the C3R encryption client in conjunction with an AWS Clean Rooms collaboration.

NOTICE: This project is released as open source under the Apache 2.0 license but is only intended for use with AWS Clean Rooms. Any other use cases may result in errors or inconsistent results.

## Table of Contents
- [Cryptographic Computing for Clean Rooms (C3R)](#cryptographic-computing-for-clean-rooms-c3r)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Downloading Releases](#downloading-releases)
    - [System Requirements](#system-requirements)
    - [Supported Data Formats](#supported-data-formats)
    - [AWS CLI Options in C3R](#aws-cli-options-in-c3r)
    - [C3R CLI Modes](#c3r-cli-modes)
      - [`schema` mode](#schema-mode)
      - [`encrypt` mode](#encrypt-mode)
      - [`decrypt` mode](#decrypt-mode)
    - [SDK Usage Examples](#sdk-usage-examples)
  - [Running C3R on Apache Spark](#running-c3r-on-apache-spark)
    - [Security Notes for running C3R on Apache Spark](#security-notes-for-running-c3r-on-apache-spark)
  - [General Security Notes](#general-security-notes)
    - [Trusted Computing Environment](#trusted-computing-environment)
    - [Temporary Files](#temporary-files)
  - [Frequently Asked Questions](#frequently-asked-questions)
    - [What data types can be encrypted?](#what-data-types-can-be-encrypted)
    - [What Parquet data types are supported?](#what-parquet-data-types-are-supported)
    - [What is an equivalence class?](#what-is-an-equivalence-class)
    - [Does the C3R encryption client implement any non-standard cryptography?](#does-the-c3r-encryption-client-implement-any-non-standard-cryptography)
  - [License](#license)


## Getting Started

### Downloading Releases

The C3R encryption client command line interface and related JARs can be downloaded from the [Releases](https://github.com/aws/c3r/releases) section of this repository. The SDK artifacts are also available on Maven's central repository.

### System Requirements

1. Java Runtime Environment version 11 or newer.

2. Enough disk storage to hold cleartext data, temporary files, and the encrypted output. See the "[Guidelines for the C3R encryption client](https://docs.aws.amazon.com/clean-rooms/latest/userguide/crypto-computing-guidelines.html)" section of the user guide for details on how settings affect storage needs.

### Supported Data Formats

CSV and Parquet file formats are supported. For CSV files, the C3R encryption client treats all values as strings. For Parquet files, the data types are listed in [What Parquet data types are supported?](#what-parquet-data-types-are-supported). See [What data types can be encrypted?](#what-data-types-can-be-encrypted) for information on encryption of particular data types. Further details and  limitations are found in the "[Supported file and data types](https://docs.aws.amazon.com/clean-rooms/latest/userguide/crypto-computing-file-types.html)" section of the user guide.

The core functionality of the C3R encryption client is format agnostic; the SDK can be used for any format by implementing an appropriate [RowReader](https://github.com/aws/c3r/blob/main/c3r-sdk-core/src/main/java/com/amazonaws/c3r/io/RowReader.java) and [RowWriter](https://github.com/aws/c3r/blob/main/c3r-sdk-core/src/main/java/com/amazonaws/c3r/io/RowWriter.java).

### AWS CLI Options in C3R

Modes which make API calls to AWS services feature optional `--profile` and `--region` flags, allowing for convenient selection of an AWS CLI named profile and AWS region respectively.

### C3R CLI Modes

The C3R encryption client is an executable JAR with a command line interface (CLI). It has several modes of operation which are described in the usage help message, e.g.:

```
  schema   Generate an encryption schema for a tabular file.
  encrypt  Encrypt tabular file content for use in a secure collaboration.
  decrypt  Decrypt tabular file content derived from a secure collaboration.
```

These modes are briefly described in the subsequent portions of this README.

#### `schema` mode

For the C3R encryption client to encrypt a tabular file for a collaboration, it must have a corresponding schema file specifying how the encrypted output should be derived from the input.

The C3R encryption client can help generate schema files for an `INPUT` file using the `schema` command. E.g.,

```
$ java -jar c3r-cli.jar schema --interactive INPUT
```

See the "[Generate an encryption schema for a tabular file](https://docs.aws.amazon.com/clean-rooms/latest/userguide/prepare-encrypted-data.html#gen-encryption-schema-csv)" section of the user guide for more information.

#### `encrypt` mode

Given the following:

1. a tabular `INPUT` file,

2. a corresponding `SCHEMA` file,

3. a collaboration `COLLABORATION_ID` in the form of a UUID, and

4. an environment variable `C3R_SHARED_SECRET` containing a Base64-encoded 256-bit secret. See the "[Preparing encrypted data tables](https://docs.aws.amazon.com/clean-rooms/latest/userguide/prepare-encrypted-data.html#create-SSK)" section of the user guide for details on how to generate a shared secret key.


An encrypted `OUTPUT` file can be generated by running the C3R encryption client at the command line as follows:

```
$ java -jar c3r-cli.jar encrypt INPUT \
  --schema=SCHEMA \
  --id=COLLABORATION_ID \
  --output=OUTPUT
```

See the "[Encrypt data](https://docs.aws.amazon.com/clean-rooms/latest/userguide/prepare-encrypted-data.html#encrypt-data)" section of the user guide for more information.

#### `decrypt` mode

Once queries have been executed on encrypted data in an AWS Clean Rooms collaboration, that encrypted query results `INPUT` file can be decrypted generating a cleartext `OUTPUT` file using the same Base64-encoded 256-bit secret stored in the `C3R_SHARED_SECRET` environment variable, and `COLLABORATION_ID` as follows:

```
$ java -jar c3r-cli.jar decrypt INPUT \
  --id=COLLABORATION_ID \
  --output=OUTPUT
```

See the "[Decrypting data tables with the C3R encryption client](https://docs.aws.amazon.com/clean-rooms/latest/userguide/decrypt-data.html)" section of the user guide.

### SDK Usage Examples

SDK usage examples are available in the SDK packages' `src/examples` directories.

## Running C3R on [Apache Spark](http://spark.apache.org/)

The `c3r-cli-spark` package is a version of `c3r-cli` which must be submitted as a job to a running [Apache Spark](http://spark.apache.org/) server.

The JAR's `com.amazonaws.c3r.spark.cli.Main` class is submitted via the Apache Spark `spark-submit` script and the JAR is then run using passed command line arguments. E.g., here is how to view the top-level usage information:

```
./spark-3.4.0-bin-hadoop3-scala2.13/bin/spark-submit \
  --master SPARK_SERVER_URL \
  ... spark-specific options omitted ... \
  --class com.amazonaws.c3r.spark.cli.Main \
  c3r-cli-spark.jar \
  --help
```

And here is how to submit a job for encryption:

```
AWS_REGION=... \
C3R_SHARED_SECRET=... \
./spark-3.4.0-bin-hadoop3-scala2.13/bin/spark-submit \
  --master SPARK_SERVER_URL \
  ... spark-specific options omitted ... \
  --class com.amazonaws.c3r.spark.cli.Main \
  c3r-cli-spark.jar \
  encrypt INPUT.parquet \
  --schema=... \
  --output=... \
  --id=...
```

### Security Notes for running C3R on Apache Spark

It is important to note that `c3r-cli-spark` makes no effort to add _additional_ encryption to data transmitted or stored in temporary files by Apache Spark. This means, for example, that on an Apache Spark server with no encryption enabled, sensitive info such as the `C3R_SHARED_SECRET` will appear in plaintext RPC calls between the server and workers. It is up to users to ensure their Apache Spark server has been configured according to their specific security needs. See the Apache Spark [security documentation](https://spark.apache.org/docs/latest/security.html) for guidance on how to configure Apache Spark server security settings.

## General Security Notes

The following is a high level description of some security concerns to keep in mind when using the C3R encryption client to encrypt data.

### Trusted Computing Environment

The shared secret key and data-to-be-encrypted is by default consumed directly from disk by the C3R encryption client on a user’s machine. It is, therefore, left to users to take any and all necessary precautions to ensure those security concerns beyond what the C3R is capable of enforcing are met. For example:

1. the machine running the C3R encryption client meets the user’s needs as a trusted computing platform,

2. the C3R encryption client is run in a minimally privileged manner and not exposed to untrusted data/networks/etc., and

3. any post-encryption cleanup/wiping of keys and/or data is performed as needed on the system post encryption.

### Temporary Files

When encrypting a source file, the C3R encryption client will create temporary files on disk. These files will be deleted when the C3R encryption client finishes generating the encrypted output. Unexpected termination of the C3R encryption client execution may prevent the C3R encryption client or JVM from deleting these files, allowing them to persist on disk. These temporary files will have all columns of type `fingerprint` or `sealed` encrypted, but some additional privacy-enhancing post-processing may not have been completed. By default, the C3R encryption client will utilize the host operating system’s temporary directory for these temporary files. If a user prefers an explicit location for such files, the optional `--tempDir=DIR` flag can specify a different location to create such files.


## Frequently Asked Questions

### What data types can be encrypted?
Currently, only string values are supported by sealed columns.

For fingerprint columns, types are grouped into [equivalence classes](#what-is-an-equivalence-class). Equivalence classes allow identical fingerprints to be assigned to the same semantic value regardless of the original representation. For example, the _integral value_ `42` will be assigned the same fingerprint regardless of whether it was originally an `SmallInt`, `Int`, or `BigInt`. No non-integral values, however, will ever be assigned the same fingerprint as the integral value `42`. 

The following equivalence classes are supported by fingerprint columns:
- `BOOLEAN`
- `DATE`
- `INTEGRAL`
- `STRING`

For CSV files, the C3R encryption client treats all values simply as UTF-8 encoded text and makes no attempt to interpret them differently prior to encryption.

For Parquet files, an error will be raised if a non-supported type for a particular column type is used.

### What Parquet data types are supported?
The C3R encryption client can process any non-complex (i.e., primitive) data in a Parquet file that represents a data type supported by Clean Rooms. The following Parquet data types are supported:
- `Binary` with the following logical annotations:
  - None if the `--parquetBinaryAsString` is set (`STRING` data type)
  - `Decimal(scale, precision)` (`DECIMAL` data type)
  - `String` (`STRING` data type)
- `Boolean` with no logical annotation (`BOOLEAN` data type)
- `Double` with no logical annotation (`DOUBLE` data type)
- `Fixed_Len_Binary_Array` with the `Decimal(scale, precision)` logical annotation (`DECIMAL` data type)
- `Float` with no logical annotation (`FLOAT` data type)
- `Int32` with the following logical annotations:
  - None (`INT` data type)
  - `Date` (`DATE` data type)
  - `Decimal(scale, precision)` (`DECIMAL` data type)
  - `Int(16, true)` (`SMALLINT` data type)
  - `Int(32, true)` (`INT` data type)
- `Int64` with the following logical annotations:
  - None (`BIGINT` data type)
  - `Decimal(scale, precision)` (`DECIMAL` data type)
  - `Int(64, true)` (`BIGINT` data type)
  - `Timestamp(isUTCAdjusted, TimeUnit.MILLIS)` (`TIMESTAMP` data type)
  - `Timestamp(isUTCAdjusted, TimeUnit.MICROS)` (`TIMESTAMP` data type)
  - `Timestamp(isUTCAdjusted, TimeUnit.NANOS)` (`TIMESTAMP` data type)

### What is an equivalence class?
An equivalence class is a set of data types that can be unambiguously compared for equality via a representative data type.

The equivalence classes are:
- `BOOLEAN` containing data types: `BOOLEAN`
- `DATE` containing data types: `DATE`
- `INTEGRAL` containing data types: `BIGINT`, `INT`, `SMALLINT`
- `STRING` containing data types: `CHAR`, `STRING`, `VARCHAR`

### Does the C3R encryption client implement any non-standard cryptography?
The C3R encryption client uses only NIST-standardized algorithms and-- with one exception-- only by calling their implementation in the Java standard cryptographic library. The sole exception is that the client has its own implementation of HKDF (from RFC5869), but using MAC algorithms from the Java standard cryptographic library.

### Does the C3R encryption client support FIPS?
Yes, the C3R encryption client supports FIPS endpoints. For more information, see the AWS documentation on [Dual-stack and FIPS endpoints](https://docs.aws.amazon.com/sdkref/latest/guide/feature-endpoints.html).

## License

This project is licensed under the Apache-2.0 License.

