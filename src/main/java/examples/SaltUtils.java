/*
 * Copyright 2015 PayPal Software Foundation
 */
package examples;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A helper class for hash-salt key.
 * 
 * @author pengzhang
 */
public class SaltUtils {

    /**
     * Int hash code to short hash code
     */
    public static short shortHashCode(int value) {
        return (short) (value ^ (value >>> 16));
    }

    /**
     * Convert short (-32768, 32767) to (0, 65535) for salt keys.
     */
    public static int toUnsignedShort(short value) {
        // short is in -32768 to 32767
        // this will return 0-65535 return as int value
        return value + Short.MAX_VALUE + 1;
    }

    public static byte[] shortToBytes(short s) {
        byte[] targets = new byte[2];
        for (int i = 0; i < 2; i++) {
            int offset = (targets.length - 1 - i) * 8;
            targets[i] = (byte) ((s >>> offset) & 0xff);
        }
        return targets;
    }

    public static byte[] merge(byte[] bytes1, byte[] bytes2) {
        byte[] results = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, results, 0, bytes1.length);
        System.arraycopy(bytes2, 0, results, bytes1.length, bytes2.length);
        return results;
    }

    public static byte[] buildSaltHashKey(String key) {
        byte[] hbaseKey = key.getBytes();
        int saltInt = SaltUtils.toUnsignedShort(SaltUtils.shortHashCode(key.hashCode()));
        // Use five chars string as salt hash key
        byte[] salt = Bytes.toBytes(String.format("%05d", saltInt));
        return SaltUtils.merge(salt, hbaseKey);
    }

    public static byte[] buildBytesSalt(String key) {
        return Bytes.toBytes(buildStringSalt(key));
    }

    public static String buildStringSalt(String key) {
        int saltInt = SaltUtils.toUnsignedShort(SaltUtils.shortHashCode(key.hashCode()));
        // Use five chars string as salt hash key
        return String.format("%05d", saltInt);
    }

    public static String decodeSaltHashKey(byte[] saltKey) {
        // Remove salt key in prefix: first five chars
        return Bytes.toString(saltKey).substring(5);
    }

    public static void main(String[] args) {

        String uuid = "bbbbb1cac54649ee86685e08d4790d88";
        String saltHashKey = SaltUtils.buildStringSalt(uuid);

        System.out.println(saltHashKey + uuid);
    }
}
