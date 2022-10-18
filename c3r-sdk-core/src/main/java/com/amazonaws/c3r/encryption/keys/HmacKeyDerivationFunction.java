// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Arrays;

/**
 * HMAC-based Key Derivation Function. Adapted from Hkdf.java in aws-dynamodb-encryption-java
 *
 * @see <a href="http://tools.ietf.org/html/rfc5869">RFC 5869</a>
 */
final class HmacKeyDerivationFunction {
    /**
     * Limit on key length relative to HMAC length.
     */
    private static final int DERIVED_KEY_LIMITER = 255;

    /**
     * Empty byte array.
     */
    private static final byte[] EMPTY_ARRAY = new byte[0];

    /**
     * Encryption algorithm to use.
     */
    private final String algorithm;

    /**
     * Cryptographic family algorithm is from.
     */
    private final Provider provider;

    /**
     * Symmetric key.
     */
    private SecretKey prk = null;

    /**
     * Returns an {@code HmacKeyDerivationFunction} object using the specified algorithm.
     *
     * @param algorithm the standard name of the requested MAC algorithm. See the Mac section in the
     *                  <a href=
     *                  "http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#Mac" >
     *                  Java Cryptography Architecture Standard Algorithm Name Documentation</a> for information
     *                  about standard algorithm names
     * @param provider  Desired Java Security API provider
     * @throws C3rIllegalArgumentException If the HMAC algorithm is not being used
     */
    private HmacKeyDerivationFunction(final String algorithm, final Provider provider) {
        if (!algorithm.startsWith("Hmac")) {
            throw new C3rIllegalArgumentException("Invalid algorithm `" + algorithm + "`. Hkdf may only be used with Hmac algorithms.");
        }
        this.algorithm = algorithm;
        this.provider = provider;
    }

    /**
     * Returns an {@code HmacKeyDerivationFunction} object using the specified algorithm.
     *
     * @param algorithm the standard name of the requested MAC algorithm. See the Mac section in the
     *                  <a href=
     *                  "http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#Mac" >
     *                  Java Cryptography Architecture Standard Algorithm Name Documentation</a> for information
     *                  about standard algorithm names
     * @return the new {@code Hkdf} object
     * @throws C3rRuntimeException If no Provider supports a MacSpi implementation for the
     *                             specified algorithm
     */
    public static HmacKeyDerivationFunction getInstance(final String algorithm) {
        try {
            // Constructed specifically to sanity-test arguments.
            final Mac mac = Mac.getInstance(algorithm);
            return new HmacKeyDerivationFunction(algorithm, mac.getProvider());
        } catch (NoSuchAlgorithmException e) {
            throw new C3rRuntimeException("Requested MAC algorithm isn't available on this system.", e);
        }
    }

    /**
     * Initializes this Hkdf with input keying material. A default salt of HashLen zeros will be used
     * (where HashLen is the length of the return value of the supplied algorithm).
     *
     * @param ikm the Input Keying Material
     */
    public void init(final byte[] ikm) {
        init(ikm, null);
    }

    /**
     * Initializes this Hkdf with input keying material and a salt. If {@code salt}
     * is {@code null} or of length 0, then a default salt of HashLen zeros will be
     * used (where HashLen is the length of the return value of the supplied algorithm).
     *
     * @param salt the salt used for key extraction (optional)
     * @param ikm  the Input Keying Material
     * @throws C3rRuntimeException If the symmetric key could not be initialized
     */
    public void init(final byte[] ikm, final byte[] salt) {
        byte[] realSalt = (salt == null) ? EMPTY_ARRAY : salt.clone();
        byte[] rawKeyMaterial = EMPTY_ARRAY;
        try {
            final Mac extractionMac = Mac.getInstance(algorithm, provider);
            if (realSalt.length == 0) {
                realSalt = new byte[extractionMac.getMacLength()];
                Arrays.fill(realSalt, (byte) 0);
            }
            extractionMac.init(new SecretKeySpec(realSalt, algorithm));
            rawKeyMaterial = extractionMac.doFinal(ikm);
            this.prk = new SecretKeySpec(rawKeyMaterial, algorithm);
        } catch (GeneralSecurityException e) {
            // We've already checked all the parameters so no exceptions
            // should be possible here.
            throw new C3rRuntimeException("Unexpected exception", e);
        } finally {
            Arrays.fill(rawKeyMaterial, (byte) 0); // Zeroize temporary array
        }
    }

    /**
     * Returns a pseudorandom key of {@code length} bytes.
     *
     * @param info   optional context and application specific information (can be a zero-length array)
     * @param length number of bytes the key should have
     * @return a pseudorandom key of {@code length} bytes
     * @throws C3rIllegalArgumentException If this object has not been initialized
     */
    public byte[] deriveKey(final byte[] info, final int length) {
        if (length < 0) {
            throw new C3rIllegalArgumentException("Length must be a non-negative value.");
        }
        assertInitialized();
        final byte[] result = new byte[length];
        final Mac mac = createMac();

        if (length > DERIVED_KEY_LIMITER * mac.getMacLength()) {
            throw new C3rIllegalArgumentException("Requested keys may not be longer than 255 times the underlying HMAC length.");
        }

        byte[] t = EMPTY_ARRAY;
        try {
            int loc = 0;
            byte i = 1;
            while (loc < length) {
                mac.update(t);
                mac.update(info);
                mac.update(i);
                t = mac.doFinal();

                for (int x = 0; x < t.length && loc < length; x++, loc++) {
                    result[loc] = t[x];
                }

                i++;
            }
        } finally {
            Arrays.fill(t, (byte) 0); // Zeroize temporary array
        }
        return result;
    }

    /**
     * Create a message authentication code using the symmetric key generated by the input keying material and salt
     * {@link #init(byte[], byte[])}.
     *
     * @return MAC generated by requested algorithm and symmetric key
     * @throws C3rRuntimeException If the MAC couldn't be generated
     */
    private Mac createMac() {
        try {
            final Mac mac = Mac.getInstance(algorithm, provider);
            mac.init(prk);
            return mac;
        } catch (NoSuchAlgorithmException | InvalidKeyException ex) {
            // We've already validated that this algorithm/key is correct.
            throw new C3rRuntimeException("Internal error: failed to create MAC for " + algorithm + ".", ex);
        }
    }

    /**
     * Throws an {@code C3rRuntimeException} if this object has not been initialized.
     *
     * @throws C3rRuntimeException If this object has not been initialized
     */
    private void assertInitialized() {
        if (prk == null) {
            throw new C3rRuntimeException("Hkdf has not been initialized");
        }
    }
}