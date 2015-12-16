package com.pinganfu.hippo.redis.paser.util;

public class RedisNumUtil {
    /**
     * byte array change to int
     * @param ary
     * @param offset  begin index of the array
     * @return int
     * */
    public static int bytesToInt(byte[] ary, int offset) {
        int value;
        value = (int) ((ary[offset] & 0xFF) | ((ary[offset + 1] & 0xFF) << 8) | ((ary[offset + 2] & 0xFF) << 16) | ((ary[offset + 3] & 0xFF) << 24));
        return value;
    }

    /** 
     * 通过byte数组取到long 
     *  
     * @param bb 
     * @param index 
     * @return 
     */
    public static long getLong(byte[] bb, int index) {
        return ((((long) bb[index + 7] & 0xff) << 56) | (((long) bb[index + 6] & 0xff) << 48) | (((long) bb[index + 5] & 0xff) << 40) | (((long) bb[index + 4] & 0xff) << 32) | (((long) bb[index + 3] & 0xff) << 24) | (((long) bb[index + 2] & 0xff) << 16) | (((long) bb[index + 1] & 0xff) << 8) | (((long) bb[index + 0] & 0xff) << 0));
    }
}
