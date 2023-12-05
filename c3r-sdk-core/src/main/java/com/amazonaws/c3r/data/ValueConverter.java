// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.UnknownNullness;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility functions to convert values from one type to another based off of column specifications.
 *
 * <p>
 * For encoded values, the byte representation follows the general form:<br/>
 * Byte 1: {@link ClientDataInfo}<br/>
 * Bytes 2-(N-1): Metadata if the type needs it to recreate the value<br/>
 * Bytes N+: Data if the value is not null<br/>
 * </p>
 */
public final class ValueConverter {
    /**
     * Private utility class constructor.
     */
    private ValueConverter() {
    }

    /**
     * Creates a basic value (i.e., one that requires no metadata) from the given {@code bytes}.
     *
     * @param bytes              Bytes representing a simple Java type
     * @param expectedByteLength How many bytes the value should have
     * @param type               Name of type to use in error messages
     * @param getter             Function to call on {@code ByteBuffer} to get the value of type {@code T} that
     *                           is expected to consume all of the `bytes`.
     * @param <T>                Java class for the basic value
     * @return The reconstructed value
     * @throws C3rRuntimeException if the number of bytes is not the expected length
     */
    private static <T> T basicFromBytes(final byte[] bytes, final int expectedByteLength, @NonNull final ClientDataType type,
                                        @NonNull final Function<ByteBuffer, T> getter) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length != expectedByteLength) {
            throw new C3rRuntimeException(type + " should be " + expectedByteLength + " in length but " + bytes.length +
                    " found.");
        }
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final T value = getter.apply(buffer);
        if (buffer.hasRemaining()) {
            throw new C3rRuntimeException(buffer.remaining() + " bytes still left but expected amount has been processed by array.");
        }
        return value;
    }

    /**
     * Creates an integral number from a byte array using {@code BigInteger} which checks for under and over flows.
     *
     * @param bytes  Bytes representing an integral value
     * @param type   Client data type name to use in error messages
     * @param getter Function to call on {@code BigInteger} to get value of type {@code T}
     * @param <T>    The type of integer to retrieve
     * @return Reconstructed integral number
     * @throws C3rRuntimeException if the value is out of range for the type
     */
    private static <T> T integralFromBytes(final byte[] bytes, @NonNull final ClientDataType type,
                                           @NonNull final Function<BigInteger, T> getter) {
        if (bytes == null) {
            return null;
        }
        try {
            final BigInteger value = new BigInteger(bytes);
            return getter.apply(value);
        } catch (final ArithmeticException e) {
            throw new C3rRuntimeException("Value out of range of a " + type + ".", e);
        }
    }

    /**
     * Creates a string from an array of UTF-8 encoded bytes.
     *
     * @param bytes UTF-8 byte array
     * @return String representation of byte value
     */
    private static java.lang.String stringFromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new java.lang.String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Converts a value of a basic type (i.e., one that has no extra metadata) into bytes.
     *
     * @param value  What to store
     * @param size   Length of the value being stored
     * @param putter Function to call on {@code ByteBuffer} to put the value in
     * @param <T>    The particular class being stored
     * @return Byte representation of value
     * @throws C3rRuntimeException If the value does not fill up the expected number of bytes
     */
    private static <T> byte[] basicToBytes(final T value, final int size, @NonNull final BiFunction<ByteBuffer, T, ByteBuffer> putter) {
        if (value == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        putter.apply(buffer, value);
        if (buffer.hasRemaining()) {
            throw new C3rRuntimeException("Too many bytes in array.");
        }
        return buffer.array();
    }

    /**
     * Turns a string into its UTF-8 byte representation.
     *
     * @param value String to convert to UTF-8 encoded bytes
     * @return Byte encoding of string value
     */
    private static byte[] stringToBytes(final java.lang.String value) {
        if (value == null) {
            return null;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Determines how many bytes will be added to the output. If the value is specified, the metadata parameters must all be specified.
     * If the value is null the metadata parameters can either all be specified as null or non-null values but not a mix.
     *
     * @param value          Value being encoded
     * @param metadataValues Array of metadata parameters
     * @param metadataLength How many bytes will be needed to store metadata
     * @param <T>            The data type for the value
     * @return Number of bytes that will be needed to store metadata.
     */
    private static <T> int getMetaDataByteLength(final T value, @NonNull final Object[] metadataValues, final int metadataLength) {
        if (value == null && Arrays.stream(metadataValues).allMatch(Objects::nonNull)) {
            return metadataLength;
        } else if (value == null) {
            return 0;
        }
        return metadataLength;
    }

    /**
     * Gets the ClientDataInfo from the start of an encoded value.
     *
     * @param buffer   Encoded C3R data type
     * @param expected Expected ClientDataType
     * @return Basic information on encoded value
     * @throws C3rRuntimeException if the encoded value doesn't have enough bytes or an unexpected type
     */
    private static ClientDataInfo stripClientDataInfo(@NonNull final ByteBuffer buffer, @NonNull final ClientDataType expected) {
        if (buffer.remaining() < ClientDataInfo.BYTE_LENGTH) {
            throw new C3rRuntimeException("Value could not be decoded, not enough bytes.");
        }
        final ClientDataInfo info = ClientDataInfo.decode(buffer.get());
        if (info.getType() != expected) {
            throw new C3rRuntimeException("Expected to decode " + expected + " but found " + info.getType() + " instead.");
        }
        return info;
    }

    /**
     * Verifies that if {@code value} is non-null that all metadata parameters are also non-null
     * or if {@code value} is null that all metadata parameters have the same nullness.
     *
     * @param value    Value to be encoded
     * @param metadata Parameters needed to correctly reconstitute the value
     * @return {@code true} if parameters are correctly specified give whether value is null or not
     */
    private static boolean metadataSpecifiedIncorrectly(final Object value, final Object[] metadata) {
        final boolean allNull = Arrays.stream(metadata).allMatch(Objects::isNull);
        final boolean allNonNull = Arrays.stream(metadata).allMatch(Objects::nonNull);
        return (value == null || !allNonNull) && (value != null || (!allNull && !allNonNull));
    }

    /**
     * Takes a value of type {@code T} and converts it to a byte array.
     *
     * @param value  Value of type {@code T} to be converted
     * @param buffer ByteBuffer used to convert the value to bytes
     * @param putter Function to call on {@code ByteBuffer} to insert the value
     * @param <T>    Type of data being stored
     * @return Byte representation of value
     */
    private static <T> byte[] encodeValue(final T value, @NonNull final ByteBuffer buffer, @NonNull final Function<T, ByteBuffer> putter) {
        if (value != null) {
            putter.apply(value);
        }
        return buffer.array();
    }

    /**
     * For types that only need to call a single function on {@code ByteBuffer} to encode the value.
     *
     * @param value  Value being encoded
     * @param type   Client data type being encoded
     * @param size   Expected size of the value in bytes
     * @param putter Function to call on {@code ByteBuffer} to store the value
     * @param <T>    Java type being converted to bytes
     * @return Byte representation of value
     */
    private static <T> byte[] basicEncode(final T value, @NonNull final ClientDataType type, @NonNull final Integer size,
                                          @NonNull final BiFunction<ByteBuffer, T, ByteBuffer> putter) {
        final ClientDataInfo info = ClientDataInfo.builder().type(type).isNull(value == null).build();
        final int length = (value == null) ? 0 : size;
        final ByteBuffer buffer = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                .put(info.encode());
        final byte[] bytes = encodeValue(value, buffer, x -> putter.apply(buffer, x));
        checkBufferHasNoRemaining(buffer);
        return bytes;
    }

    /**
     * Encodes a string based value and its length into a byte array. Length is included to verify correct decoding of the value.
     *
     * <p>
     * For a non-null value, the encoded byte array is of the form:<br/>
     * Byte 1: {@link ClientDataInfo}<br/>
     * Bytes 2-5: Length of the String<br/>
     * Bytes 6+: Bytes for the UTF-8 formatted version of the string
     * </p>
     *
     * <p>
     * For a null value, the encoded byte array is of the form:<br/>
     * Byte 1: {@code ClientDataInfo}
     * </p>
     *
     * @param type  The specific C3R string based data type
     * @param value String value
     * @return Byte array with all the information to correctly reconstruct the string
     */
    private static byte[] encodeString(@NonNull final ClientDataType type, final java.lang.String value) {
        final ClientDataInfo info = ClientDataInfo.builder().type(type).isNull(value == null).build();
        final byte[] bytes = stringToBytes(value);
        final int length = (bytes == null) ? 0 : bytes.length;
        final ByteBuffer buffer = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                .put(info.encode());
        if (value != null) {
            buffer.put(bytes);
        }
        checkBufferHasNoRemaining(buffer);
        return buffer.array();
    }

    /**
     * Decodes a value that only needs a single call to {@code ByteBuffer} to recreate the value.
     *
     * @param bytes  Byte representation of value
     * @param type   Name of client data type to use in error messages
     * @param getter Function to call on {@code ByteBuffer} to get the value
     * @param <T>    The Java type being created
     * @return Value created from bytes
     * @throws C3rRuntimeException if the number of bytes is wrong for the type
     */
    private static <T> T basicDecode(final byte[] bytes, @NonNull final ClientDataType type,
                                     @NonNull final Function<ByteBuffer, T> getter) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final ClientDataInfo info = stripClientDataInfo(buffer, type);
        T value = null;
        if (!info.isNull()) {
            try {
                value = getter.apply(buffer);
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
        checkBufferHasNoRemaining(buffer);
        return value;
    }

    /**
     * Decodes a byte array into the original string based value. Verifies the string is of the expected length.
     *
     * @param type  The specific C3R string based data type
     * @param bytes Encoded string based value with information needed to recreate the correct value
     * @return Decoded String value
     * @throws C3rRuntimeException if not enough bytes are present, the wrong type is found or the length checks fail
     */
    private static java.lang.String stringDecode(@NonNull final ClientDataType type, final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final ClientDataInfo info = stripClientDataInfo(buffer, type);
        java.lang.String value = null;
        if (!info.isNull()) {
            try {
                final byte[] strBytes = new byte[buffer.remaining()];
                buffer.get(strBytes);
                value = stringFromBytes(strBytes);
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
        checkBufferHasNoRemaining(buffer);
        return value;
    }

    /**
     * Takes a boolean value to a byte array.
     *
     * @param value Boolean to turn into bytes
     * @return Byte array of 1 byte that stores a representation of the boolean value
     */
    private static byte[] booleanToBytes(final boolean value) {
        if (value) {
            return new byte[]{(byte) 1};
        } else {
            return new byte[]{(byte) 0};
        }
    }

    /**
     * Converts a byte value to a boolean value.
     *
     * @param value Boolean value as a byte
     * @return The boolean value the byte represents
     * @throws C3rIllegalArgumentException if the byte does not represent a valid boolean value
     */
    private static boolean booleanFromByte(final byte value) {
        if (value == (byte) 0) {
            return false;
        } else if (value == (byte) 1) {
            return true;
        } else {
            throw new C3rIllegalArgumentException("Could not decode boolean value from byte.");
        }
    }

    /**
     * Checks to confirm all bytes have been read from the {@code ByteBuffer}.
     *
     * @param buffer {@code ByteBuffer} that should have no remaining bytes to read
     * @throws C3rRuntimeException If there are still bytes left in the array
     */
    private static void checkBufferHasNoRemaining(@NonNull final ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            throw new C3rRuntimeException(buffer.remaining() + " bytes still left but expected number of bytes have been decoded.");
        }
    }

    /**
     * Gets the data type from an encoded value.
     *
     * @param bytes Byes containing an encoded value and its metadata
     * @return C3R data type for the value
     */
    public static ClientDataType clientDataTypeForEncodedValue(final byte[] bytes) {
        if (bytes != null && bytes.length >= ClientDataInfo.BYTE_LENGTH) {
            return ClientDataInfo.decode(bytes[0]).getType();
        }
        return ClientDataType.UNKNOWN;
    }

    /**
     * Find the equivalence class super type for the value and return the value represented in that byte format.
     *
     * @param value Value to convert to equivalence class super type
     * @return byte representation of value in super class
     */
    private static byte[] getBytesForFingerprint(@NonNull final Value value) {
        final ClientDataType superType = value.getClientDataType().getRepresentativeType();
        return value.getBytesAs(superType);
    }

    /**
     * Gets the byte representation of the value according to the column type and any specified conversions.
     *
     * @param value      Value to get bytes from
     * @param columnType Type of column being written
     * @return byte representation of value in the form of the desired {@code ClientDataType}
     */
    public static byte[] getBytesForColumn(@NonNull final Value value, @NonNull final ColumnType columnType) {
        if (columnType == ColumnType.FINGERPRINT) {
            return getBytesForFingerprint(value);
        } else {
            return value.getBytes();
        }
    }

    /**
     * Gets the target data type of the column.
     *
     * @param value      Input value
     * @param columnType Type of column being written
     * @return {@code ClientDataType} contained in column output
     */
    public static ClientDataType getClientDataTypeForColumn(@NonNull final Value value, @NonNull final ColumnType columnType) {
        if (columnType == ColumnType.FINGERPRINT) {
            return value.getClientDataType().getRepresentativeType();
        } else {
            return value.getClientDataType();
        }
    }

    /**
     * Utility functions for converting a C3R BigInt to and from various representations.
     */
    public static final class BigInt {
        /**
         * Convert a big-endian formatted byte array to its long value.
         * Byte array must be {@value Long#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding long value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Long fromBytes(final byte[] bytes) {
            return integralFromBytes(bytes, ClientDataType.BIGINT, BigInteger::longValueExact);
        }

        /**
         * Convert an int value to a long big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Integer value) {
            if (value == null) {
                return null;
            }
            return toBytes(value.longValue());
        }

        /**
         * Convert a long value to its big-endian byte representation.
         *
         * @param value Long
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Long value) {
            return basicToBytes(value, ClientDataType.BIGINT_BYTE_SIZE, ByteBuffer::putLong);
        }

        /**
         * Encodes a BigInt value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value BigInt value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Long value) {
            return basicEncode(value, ClientDataType.BIGINT, ClientDataType.BIGINT_BYTE_SIZE, ByteBuffer::putLong);
        }

        /**
         * Decodes a byte array into the original BigInt value.
         *
         * @param bytes Encoded value and metadata
         * @return BigInt value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not BigInt
         */
        public static Long decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.BIGINT, ByteBuffer::getLong);
        }
    }

    /**
     * Utility functions for converting a C3R Boolean to and from various representations.
     */
    public static final class Boolean {
        /**
         * Convert value to boolean.
         *
         * @param bytes byte encoded value
         * @return {@code true} if value is non-zero or {@code false}
         * @throws C3rRuntimeException if the byte length is not the expected amount
         */
        @UnknownNullness
        public static java.lang.Boolean fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            } else if (bytes.length != 1) {
                throw new C3rRuntimeException("Boolean value expected to be a single byte.");
            }
            return booleanFromByte(bytes[0]);
        }

        /**
         * Take a boolean value and convert it to a 1 byte long byte array.
         *
         * @param value {@code true}, {@code false} or {@code null}
         * @return {@code 1}, {@code 0} or {@code null}
         */
        public static byte[] toBytes(final java.lang.Boolean value) {
            if (value == null) {
                return null;
            }
            return booleanToBytes(value);
        }

        /**
         * Encodes a Boolean value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Boolean value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Boolean value) {
            return basicEncode(toBytes(value), ClientDataType.BOOLEAN, Byte.BYTES, ByteBuffer::put);
        }

        /**
         * Decodes a byte array into the original Boolean value.
         *
         * @param bytes Encoded value and metadata
         * @return Boolean value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Boolean
         */
        @UnknownNullness
        public static java.lang.Boolean decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.BOOLEAN);
            java.lang.Boolean value = null;
            if (!info.isNull()) {
                try {
                    final byte[] remaining = new byte[buffer.remaining()];
                    buffer.get(remaining);
                    value = fromBytes(remaining);
                } catch (BufferUnderflowException e) {
                    throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
                }
            }
            checkBufferHasNoRemaining(buffer);
            return value;
        }
    }

    /**
     * Utility functions for converting a C3R Char to and from various representations.
     */
    public static final class Char {
        /**
         * Converts bytes to a fixed length character array.
         *
         * @param bytes UTF-8 encoded bytes to convert
         * @return Fixed length character array
         */
        public static java.lang.String fromBytes(final byte[] bytes) {
            return stringFromBytes(bytes);
        }

        /**
         * Convert a fixed length character array to a byte array.
         *
         * @param value Character ta turn into UTF-8 encoded bytes
         * @return UTF-8 byte encoding of string
         */
        public static byte[] toBytes(final java.lang.String value) {
            return stringToBytes(value);
        }

        /**
         * Encodes a fixed length character array along with necessary metadata to reconstitute the value for encryption.
         * See {@link ValueConverter#encodeString} for byte format.
         *
         * @param value Character array  to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.String value) {
            return encodeString(ClientDataType.CHAR, value);
        }

        /**
         * Decodes a byte array into the original Char value.
         *
         * @param bytes Encoded value and metadata
         * @return Char value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Char
         */
        public static java.lang.String decode(final byte[] bytes) {
            return stringDecode(ClientDataType.CHAR, bytes);
        }
    }

    /**
     * Utility functions for converting a C3R Date to and from various representations. Date is relative to epoch.
     */
    public static final class Date {
        /**
         * Get the number of day ticks since epoch.
         *
         * @param bytes Byte representation of the ticks
         * @return Number of days since epoch
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        public static Integer fromBytes(final byte[] bytes) {
            return basicFromBytes(bytes, Integer.BYTES, ClientDataType.DATE, ByteBuffer::getInt);
        }

        /**
         * Converts the number of ticks since epoch to its byte representation.
         *
         * @param value Number of ticks since epoch
         * @return Byte representation of number
         */
        public static byte[] toBytes(final Integer value) {
            return Int.toBytes(value);
        }

        /**
         * Encodes a Date value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Date value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Integer value) {
            return basicEncode(value, ClientDataType.DATE, Integer.BYTES, ByteBuffer::putInt);
        }

        /**
         * Decodes a byte array into the original Date value.
         *
         * @param bytes Encoded value and metadata
         * @return Date value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Date
         */
        public static Integer decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.DATE, ByteBuffer::getInt);
        }
    }

    /**
     * Utility functions for converting a C3R Decimal to and from various representations.
     */
    public static final class Decimal {
        /**
         * How many bytes are needed to store the metadata needed to recreate the original value.
         */
        private static final int TOTAL_METADATA_BYTES = 2 * Integer.BYTES;

        /**
         * Checks that the scale and precision on the value are within the bounds of the specified scale and precision.
         *
         * @param specifiedPrecision Precision in metadata
         * @param valuePrecision     Precision of decoded value
         * @param specifiedScale     Scale in metadata
         * @param valueScale         Scale of decoded value
         * @return {@code true} if the decoded scale and precision are within the bounds of the values in the metadata.
         */
        private static boolean isValueInvalid(final int specifiedPrecision, final int valuePrecision,
                                              final int specifiedScale, final int valueScale) {
            return (specifiedPrecision < valuePrecision) ||
                    (specifiedScale > 0 && specifiedScale < valueScale) ||
                    (specifiedScale < 0 && specifiedScale > valueScale) ||
                    (specifiedScale == 0 && valueScale != 0);
        }

        /**
         * Takes a byte array and returns the fixed point number it represents. This is done by transforming the byte array into
         * a string and then a {@code BigDecimal} value. Precision and scale may be smaller than they were originally since they
         * are calculated using the digits present in the string value only.
         *
         * @param bytes Bytes representing a floating
         * @return Fixed point number
         */
        public static BigDecimal fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            final java.lang.String value = new java.lang.String(bytes, StandardCharsets.UTF_8);
            return new BigDecimal(value);
        }

        /**
         * Takes a fixed point number and returns the byte representation of the value. This is done by transforming the value
         * into a string so the scale and precision before turning it into bytes may be larger than when decoded since the
         * decoded value will only look at the digits present.
         *
         * @param decimal Fixed point number
         * @return Byte representation of raw fixed point number
         */
        public static byte[] toBytes(final BigDecimal decimal) {
            if (decimal == null) {
                return null;
            }
            return decimal.toString().getBytes(StandardCharsets.UTF_8);
        }

        /**
         * Encodes a decimal value, the precision and scale being used into a byte array so it can be recreated exactly later.
         *
         * <p>
         * For a non-null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@link ClientDataInfo}<br/>
         * Bytes 2-5: Precision<br/>
         * Bytes 6-9: Scale<br/>
         * Bytes 10+: Bytes representing fixed point number
         * </p>
         *
         * <p>
         * For a null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@code ClientDataInfo}<br/>
         * Optionally:
         * Bytes 2-5: Precision<br/>
         * Bytes 6-9: Scale<br/>
         * </p>
         *
         * @param value     Fixed point number
         * @param precision How many digits are in the number
         * @param scale     How many digits are to the right of the decimal point
         * @return Byte array encoding all the information to recreate the decimal value
         * @throws C3rIllegalArgumentException if precision and scale are missing for a non-null value
         *                                     or have non-matching null status for a null value
         * @throws C3rRuntimeException if value is outside the limits imposed by precision and scale
         */
        public static byte[] encode(final BigDecimal value, final Integer precision, final Integer scale) {
            final Object[] metadata = new Object[]{precision, scale};
            if (metadataSpecifiedIncorrectly(value, metadata)) {
                throw new C3rIllegalArgumentException("Precision and scale must both be specified unless value is null, " +
                        "then they may optionally be null.");
            }
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.DECIMAL).isNull(value == null).build();
            final byte[] plainString = (value == null) ? null : value.toPlainString().getBytes(StandardCharsets.UTF_8);
            int length = getMetaDataByteLength(value, metadata, TOTAL_METADATA_BYTES);
            length += (value == null) ? 0 : plainString.length;
            final ByteBuffer buffer = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (value != null) {
                if (isValueInvalid(precision, value.precision(), scale, value.scale())) {
                    throw new C3rRuntimeException("Value is outside of the limits imposed by precision and scale");
                }
                buffer.putInt(precision);
                buffer.putInt(scale);
                buffer.put(plainString);
                return buffer.array();
            } else if (precision != null && scale != null) {
                buffer.putInt(precision);
                buffer.putInt(scale);
            }
            checkBufferHasNoRemaining(buffer);
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original decimal value. Verifies valid values for UTC flag and units.
         *
         * @param bytes Encoded timestamp with information needed to recreate the correct value
         * @return Information needed to create the value
         * @throws C3rRuntimeException if not enough bytes are present, the bytes do not represent a {@code BigDecimal} value
         *                             or the value is outside the limits imposed by precision and scale
         */
        public static ClientValueWithMetadata.Decimal decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.DECIMAL);
            try {
                Integer precision = null;
                Integer scale = null;
                if (buffer.remaining() >= TOTAL_METADATA_BYTES) {
                    precision = buffer.getInt();
                    scale = buffer.getInt();
                }
                BigDecimal value = null;
                if (!info.isNull()) {
                    final byte[] strBytes = new byte[buffer.remaining()];
                    buffer.get(strBytes);
                    final java.lang.String stringValue = stringFromBytes(strBytes);
                    value = new BigDecimal(stringValue);
                    if (isValueInvalid(precision, value.precision(), scale, value.scale())) {
                        throw new C3rRuntimeException("Value is outside of the limits imposed by precision and scale");
                    }
                }
                checkBufferHasNoRemaining(buffer);
                return new ClientValueWithMetadata.Decimal(value, precision, scale);
            } catch (BufferUnderflowException bue) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", bue);
            } catch (NumberFormatException nfe) {
                throw new C3rRuntimeException("Value could not be decoded, invalid format.", nfe);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Double to and from various representations.
     */
    public static final class Double {
        /**
         * Converts a big-endian formatted byte array to its double value.
         * Number of bytes must be {@value java.lang.Double#BYTES}.
         *
         * @param bytes Bytes in big-endian format
         * @return Corresponding float value
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        public static java.lang.Double fromBytes(final byte[] bytes) {
            return basicFromBytes(bytes, java.lang.Double.BYTES, ClientDataType.DOUBLE, ByteBuffer::getDouble);
        }

        /**
         * Convert a double value to its big-endian byte representation.
         *
         * @param value Double
         * @return Big-endian encoding of value
         */
        public static byte[] toBytes(final java.lang.Double value) {
            return basicToBytes(value, java.lang.Double.BYTES, ByteBuffer::putDouble);
        }

        /**
         * Encodes a Double value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Double value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Double value) {
            return basicEncode(value, ClientDataType.DOUBLE, java.lang.Double.BYTES, ByteBuffer::putDouble);
        }

        /**
         * Decodes a byte array into the original Double value.
         *
         * @param bytes Encoded value and metadata
         * @return Double value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Double
         */
        public static java.lang.Double decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.DOUBLE, ByteBuffer::getDouble);
        }
    }

    /**
     * Utility functions for converting a C3R Float to and from various representations.
     */
    public static final class Float {
        /**
         * Converts big-endian formatted bytes to float value.
         * Number of bytes must be {@value java.lang.Float#BYTES}.
         *
         * @param bytes Bytes in big-endian format
         * @return Corresponding float value
         * @throws C3rRuntimeException If the byte array is not the expected length
         */
        public static java.lang.Float fromBytes(final byte[] bytes) {
            return basicFromBytes(bytes, java.lang.Float.BYTES, ClientDataType.FLOAT, ByteBuffer::getFloat);
        }

        /**
         * Convert a float value to its big-endian byte representation.
         *
         * @param value Float
         * @return Big-endian encoding of value
         */
        public static byte[] toBytes(final java.lang.Float value) {
            return basicToBytes(value, java.lang.Float.BYTES, ByteBuffer::putFloat);
        }

        /**
         * Encodes a Float value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Float value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.Float value) {
            return basicEncode(value, ClientDataType.FLOAT, java.lang.Float.BYTES, ByteBuffer::putFloat);
        }

        /**
         * Decodes a byte array into the original Float value.
         *
         * @param bytes Encoded value and metadata
         * @return Float value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Float
         */
        public static java.lang.Float decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.FLOAT, ByteBuffer::getFloat);
        }
    }

    /**
     * Utility functions for converting a C3R Int to and from various representations.
     */
    public static final class Int {
        /**
         * Convert a big-endian formatted byte array to its integer value.
         * Byte array must be {@value Integer#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding integer value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Integer fromBytes(@Nullable final byte[] bytes) {
            return integralFromBytes(bytes, ClientDataType.INT, BigInteger::intValueExact);
        }

        /**
         * Convert an integer value to its big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Integer value) {
            return basicToBytes(value, ClientDataType.INT_BYTE_SIZE, ByteBuffer::putInt);
        }

        /**
         * Encodes an Int value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value Int value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Integer value) {
            return basicEncode(value, ClientDataType.INT, ClientDataType.INT_BYTE_SIZE, ByteBuffer::putInt);
        }

        /**
         * Decodes a byte array into the original Int value.
         *
         * @param bytes Encoded value and metadata
         * @return Int value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not Int
         */
        public static Integer decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.INT, ByteBuffer::getInt);
        }
    }

    /**
     * Utility functions for converting a C3R SmallInt to and from various representations.
     */
    public static final class SmallInt {
        /**
         * Convert a big-endian formatted byte array to its integer value.
         * Byte array must be {@value Integer#BYTES} or less in length.
         *
         * @param bytes Big-endian formatted byte array
         * @return Corresponding integer value
         * @throws C3rRuntimeException If the byte array is more than the max length
         */
        public static Short fromBytes(@Nullable final byte[] bytes) {
            return integralFromBytes(bytes, ClientDataType.SMALLINT, BigInteger::shortValueExact);
        }

        /**
         * Convert an integer value to its big-endian byte representation.
         *
         * @param value Integer
         * @return Big-endian byte encoding of value
         */
        public static byte[] toBytes(final Short value) {
            return basicToBytes(value, ClientDataType.SMALLINT_BYTE_SIZE, ByteBuffer::putShort);
        }

        /**
         * Encodes a SmallInt value along with necessary metadata to reconstitute the value for encryption.
         *
         * @param value SmallInt value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final Short value) {
            return basicEncode(value, ClientDataType.SMALLINT, ClientDataType.SMALLINT_BYTE_SIZE, ByteBuffer::putShort);
        }

        /**
         * Decodes a byte array into the original SmallInt value.
         *
         * @param bytes Encoded value and metadata
         * @return SmallInt value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not SmallInt
         */
        public static Short decode(final byte[] bytes) {
            return basicDecode(bytes, ClientDataType.SMALLINT, ByteBuffer::getShort);
        }
    }

    /**
     * Utility functions for converting a C3R String to and from various representations.
     */
    public static final class String {
        /**
         * Convert the byte array to a UTF-8 String.
         *
         * @param bytes Bytes representing string value
         * @return UTF-8 string generated from bytes
         */
        public static java.lang.String fromBytes(final byte[] bytes) {
            return stringFromBytes(bytes);
        }

        /**
         * Convert a string to the UTF-8 bytes that represent its value.
         *
         * @param value String to convert to bytes
         * @return UTF-8 byte representation
         */
        public static byte[] toBytes(final java.lang.String value) {
            return stringToBytes(value);
        }

        /**
         * Encodes a String value along with necessary metadata to reconstitute the value for encryption.
         * See {@link ValueConverter#encodeString} for byte format.
         *
         * @param value String value to encrypt
         * @return Byte representation of the value and its metadata
         */
        public static byte[] encode(final java.lang.String value) {
            return encodeString(ClientDataType.STRING, value);
        }

        /**
         * Decodes a byte array into the original String value.
         *
         * @param bytes Encoded value and metadata
         * @return String value
         * @throws C3rRuntimeException if not enough bytes are in the encoded value or the data type is not String
         */
        public static java.lang.String decode(final byte[] bytes) {
            return stringDecode(ClientDataType.STRING, bytes);
        }
    }

    /**
     * Utility functions for converting a C3R Timestamp to and from various representations. Timestamps are relative to epoch.
     */
    public static final class Timestamp {
        /**
         * Number of bytes the metadata uses.
         */
        private static final int TOTAL_METADATA_BYTES = Byte.BYTES + Integer.BYTES;

        /**
         * Takes a byte array representing a {@code BigInteger} value and converts it back to a {@code BigInteger} value
         * which is a timestamp value in nanoseconds.
         *
         * @param bytes Byte array used by a {@code BigInteger} to store a nanosecond-based timestamp
         * @return A timestamp relative to epoch in nanoseconds
         */
        public static BigInteger fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new BigInteger(bytes);
        }

        /**
         * Converts a timestamp into its raw byte representation. All values are converted to nanoseconds as unit information
         * is not conserved and using nanoseconds prevents loss of information.
         *
         * @param value Timestamp value
         * @param units What unit of time the value is in
         * @return Bytes from a {@code BigInteger} that represents the timestamp in nanoseconds
         */
        public static byte[] toBytes(final Long value, final Units.Seconds units) {
            if (value == null) {
                return null;
            }
            final BigInteger asNanos = Units.Seconds.convert(BigInteger.valueOf(value), units, Units.Seconds.NANOS);
            return asNanos.toByteArray();
        }

        /**
         * Takes a timestamp value plus if the value is in UTC time and unit for the value and creates a byte array with
         * all the information needed to recreate the value.
         *
         * <p>
         * For a non-null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@link ClientDataInfo}<br/>
         * Byte 2: Whether the timestamp is in UTC<br/>
         * Bytes 3-6: Unit of time for the value<br/>
         * Bytes 7-14: Timestamp value
         * </p>
         *
         * <p>
         * For a null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@code ClientDataInfo}<br/>
         * Optionally:
         * Byte 2: Whether the timestamp is in UTC<br/>
         * Bytes 3-6: Unit of time for the value<br/>
         * </p>
         *
         * @param value Timestamp value
         * @param isUtc If the timestamp is in UTC time or not
         * @param unit  What time unit the value is in
         * @return Byte array with all information needed to reconstruct the value as intended
         * @throws C3rIllegalArgumentException if the UTC or unit information is missing for a non-null value
         *                                     or only one value is specified when value is null
         */
        public static byte[] encode(final Long value, final java.lang.Boolean isUtc, final Units.Seconds unit) {
            final Object[] metadata = new Object[]{isUtc, unit};
            if (metadataSpecifiedIncorrectly(value, metadata)) {
                throw new C3rIllegalArgumentException("isUtc and unit must all be specified " +
                        "unless value is null then they may optionally be null.");
            }
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.TIMESTAMP).isNull(value == null).build();
            int length = ClientDataInfo.BYTE_LENGTH;
            length += getMetaDataByteLength(value, metadata, TOTAL_METADATA_BYTES);
            length += (value == null) ? 0 : Long.BYTES;
            final ByteBuffer buffer = ByteBuffer.allocate(length).put(info.encode());
            if (buffer.remaining() >= TOTAL_METADATA_BYTES) {
                buffer.put(booleanToBytes(isUtc));
                buffer.putInt(unit.ordinal());
            }
            if (!info.isNull()) {
                buffer.putLong(value);
            }
            checkBufferHasNoRemaining(buffer);
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original timestamp value. Verifies valid values for UTC flag and units.
         *
         * @param bytes Encoded timestamp with information needed to recreate the correct value
         * @return Information needed to create the value
         * @throws C3rRuntimeException if not enough bytes are present, the wrong type is found or metadata fails verification
         */
        public static ClientValueWithMetadata.Timestamp decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.TIMESTAMP);
            try {
                java.lang.Boolean isUtc = null;
                Units.Seconds unit = null;
                if (buffer.remaining() >= TOTAL_METADATA_BYTES) {
                    isUtc = booleanFromByte(buffer.get());
                    final int index = buffer.getInt();
                    if (index >= Units.Seconds.values().length) {
                        throw new C3rRuntimeException("Could not decode unit for timestamp.");
                    }
                    unit = Units.Seconds.values()[index];
                }
                Long value = null;
                if (!info.isNull()) {
                    value = buffer.getLong();
                }
                checkBufferHasNoRemaining(buffer);
                return new ClientValueWithMetadata.Timestamp(value, isUtc, unit);
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }

    /**
     * Utility functions for converting a C3R Varchar to and from various representations.
     */
    public static final class Varchar {
        /**
         * How many bytes are needed to store the metadata.
         */
        private static final int TOTAL_METADATA_BYTES = Integer.BYTES;

        /**
         * Convert the byte array into a UTF-8 variable length character array.
         *
         * @param bytes Bytes representing a variable length character array
         * @return Variable length character array
         */
        public static java.lang.String fromBytes(final byte[] bytes) {
            return stringFromBytes(bytes);
        }

        /**
         * Converts a variable length character array into its UTF-8 byte representation.
         *
         * @param value Variable length character array
         * @return Byte array that holds the UTF-8 encoding of the character array
         */
        public static byte[] toBytes(final java.lang.String value) {
            return stringToBytes(value);
        }

        /**
         * Takes a variable length character array value and creates a byte array with all the information needed to recreate the value.
         *
         * <p>
         * For a non-null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@link ClientDataInfo}<br/>
         * Bytes 2-5: Maximum length the array can be<br/>
         * Bytes 6-9: Length of the string (Used to validate data was properly decoded)<br/>
         * Bytes 10+: Bytes for the UTF-8 formatted version of the variable length character array
         * </p>
         *
         * <p>
         * For a null value, the encoded byte array is of the form:<br/>
         * Byte 1: {@code ClientDataInfo}<br/>
         * Optionally:<br/>
         * Bytes 2-5: Maximum length the array can be
         * </p>
         *
         * @param value     Character array value
         * @param maxLength Longest length the variable length character array can be
         * @return Byte array with all information needed to reconstruct the value as intended
         * @throws C3rIllegalArgumentException if the value is longer than the allowed maximum length or the maximum length is unspecified
         */
        public static byte[] encode(final java.lang.String value, final Integer maxLength) {
            final Object[] metadata = new Object[]{maxLength};
            if (metadataSpecifiedIncorrectly(value, metadata)) {
                throw new C3rIllegalArgumentException("Max length must be specified unless value is null.");
            }
            if (value != null && value.length() > maxLength) {
                throw new C3rIllegalArgumentException("Value is greater than allowed maximum length for varchar field.");
            }
            final ClientDataInfo info = ClientDataInfo.builder().type(ClientDataType.VARCHAR).isNull(value == null).build();
            int length = getMetaDataByteLength(value, metadata, TOTAL_METADATA_BYTES);
            length += (value == null) ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
            final ByteBuffer buffer = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + length)
                    .put(info.encode());
            if (maxLength != null) {
                buffer.putInt(maxLength);
            }
            if (value != null) {
                buffer.put(value.getBytes(StandardCharsets.UTF_8));
            }
            checkBufferHasNoRemaining(buffer);
            return buffer.array();
        }

        /**
         * Decodes a byte array into the original variable length character array. Verifies the string is of the expected length and
         * less than or equal to the maximum length.
         *
         * @param bytes Encoded variable length character array with information needed to recreate the correct value
         * @return Information needed to create the value
         * @throws C3rRuntimeException if not enough bytes are present, the wrong type is found or the length checks fail
         */
        public static ClientValueWithMetadata.Varchar decode(final byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final ClientDataInfo info = stripClientDataInfo(buffer, ClientDataType.VARCHAR);
            try {
                Integer maxLength = null;
                if (buffer.remaining() >= TOTAL_METADATA_BYTES) {
                    maxLength = buffer.getInt();
                }
                java.lang.String value = null;
                if (!info.isNull()) {
                    final byte[] strBytes = new byte[buffer.remaining()];
                    buffer.get(strBytes);
                    value = stringFromBytes(strBytes);
                    if (value.length() > maxLength) {
                        throw new C3rRuntimeException("Varchar expected to be " + maxLength + " characters long at most but was " +
                                value.length() + " characters long.");
                    }
                }
                checkBufferHasNoRemaining(buffer);
                return new ClientValueWithMetadata.Varchar(value, maxLength);
            } catch (BufferUnderflowException e) {
                throw new C3rRuntimeException("Value could not be decoded, not enough bytes.", e);
            }
        }
    }
}
