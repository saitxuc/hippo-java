package com.hippo.common.util;

/**
 * Created by Owen on 2015/12/28.
 */
public class KeyUtil {

    public static byte[] getByteAccordingOffset(byte[] originalKey, int offset, byte separator, int defaultLength) {
        int byteSizeLeft = getByteSizeLeft(originalKey, defaultLength);
        int blockOffset = offset / (byteSizeLeft * 8);
        byte[] offsetPerBlock = Logarithm.intToBytes((blockOffset + 1) * byteSizeLeft);
        return getKeyAfterCombineOffset(originalKey, offsetPerBlock, separator);
    }

    public static int getByteSizeLeft(byte[] originalKey, int defaultLength) {
        return defaultLength - 30 - originalKey.length - 1;
    }

    public static byte[] getKeyAfterCombineOffset(byte[] originalKey, byte[] suffix, byte sep) {
        final byte[] newKey = new byte[originalKey.length + suffix.length + 1];
        System.arraycopy(originalKey, 0, newKey, 0, originalKey.length);
        newKey[originalKey.length] = sep;
        System.arraycopy(suffix, 0, newKey, originalKey.length, suffix.length);
        return newKey;
    }
}
