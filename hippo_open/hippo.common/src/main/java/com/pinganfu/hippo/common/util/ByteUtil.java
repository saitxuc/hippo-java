package com.pinganfu.hippo.common.util;

public class ByteUtil {
    public static boolean isSame(byte[] key1, byte[] key2) {
        if (key1 == null || key2 == null) {
            return false;
        }

        if (key1.length != key2.length) {
            return false;
        } else {
            for (int _index = 0; _index < key1.length; _index++) {
                if (key1[_index] != key2[_index]) {
                    return false;
                }
            }
        }
        return true;
    }

    public static byte[] parseBoolean(boolean val) {
        return new byte[]{(byte) (val ? 1 : 0)};
    }
}
