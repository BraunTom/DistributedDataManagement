package de.hpi.ddm.structures;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class SHA256Hash {
    private final int SHA256_DIGEST_LENGTH = 32;
    private byte[] bytes = new byte[SHA256_DIGEST_LENGTH];

    private static ThreadLocal<MessageDigest> sha256Hasher = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });


    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null || getClass() != other.getClass())
            return false;
        SHA256Hash that = (SHA256Hash) other;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        // Just join the first 4 bytes of the SHA256 hash to build the hash for Java's hash
        return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
    }

    public void fromHexString(String hexString) throws Exception {
        if (hexString.length() != 2*SHA256_DIGEST_LENGTH)
            throw new Exception("A SHA256 hex string should have " + 2*SHA256_DIGEST_LENGTH + " characters.");

        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
            byte hiNibble =  hexCharToNibble(hexString.charAt(i*2));
            byte loNibble =  hexCharToNibble(hexString.charAt(i*2+1));

            bytes[i] = (byte)((hiNibble << 4) | loNibble);
        }
    }

    public void fromData(byte[] data, int length) throws Exception {
        MessageDigest digest = sha256Hasher.get();
        digest.update(data, 0, length);
        digest.digest(bytes, 0, SHA256_DIGEST_LENGTH);
    }

    @Override
    public String toString() {
        StringBuilder hexStringBuilder = new StringBuilder(2*SHA256_DIGEST_LENGTH);
        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
            hexStringBuilder.append(nibbleToHexChar((byte)(bytes[i] >> 4)));
            hexStringBuilder.append(nibbleToHexChar((byte)(bytes[i] & 0xf)));
        }

        return hexStringBuilder.toString();
    }

    private static byte hexCharToNibble(char c) throws Exception {
        if (c >= '0' && c <= '9')
            return (byte)(c - '0');
        if (c >= 'a' && c <= 'f')
            return (byte)((c - 'a') + 10);
        if (c >= 'A' && c <= 'F')
            return (byte)((c - 'A') + 10);
        throw new Exception("Invalid character in SHA256 hex string");
    }

    private static char nibbleToHexChar(byte nibble) {
        return (nibble < 10) ? (char)('0' + nibble) : (char)('a' + (nibble - 10));
    }
}
