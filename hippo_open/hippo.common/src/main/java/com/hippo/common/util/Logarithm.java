package com.hippo.common.util;

import java.nio.ByteBuffer;

public class Logarithm {
    public static double log(double value, double base) {
        return Math.log(value) / Math.log(base);
    }

    /**
     * byte array change to int
     * @param ary
     * @param offset  begin index of the array
     * @return int
     * */
    public static int bytesToInt(byte[] ary, int offset) {
        int value;
        value = (int) ((ary[offset+3] & 0xFF) | ((ary[offset + 2] << 8) & 0xFF00) | ((ary[offset + 1] << 16) & 0xFF0000) | ((ary[offset] << 24) & 0xFF000000));
        return value;
    }

    /**
     * int change to byte array
     * @param int
     * @return byte[]
     * */
    public static byte[] intToBytes(int value) {
        byte[] byte_src = new byte[4];
        byte_src[0] = (byte) ((value & 0xFF000000) >> 24);
        byte_src[1] = (byte) ((value & 0x00FF0000) >> 16);
        byte_src[2] = (byte) ((value & 0x0000FF00) >> 8);
        byte_src[3] = (byte) ((value & 0x000000FF));
        return byte_src;
    }

    /** 
     * 转换long型为byte数组 
     *  
     * @param bb 
     * @param x 

     */
    public static byte[] putLong(long x) {
        byte[] longByte = new byte[8];
        longByte[0] = (byte) (x >> 56);
        longByte[1] = (byte) (x >> 48);
        longByte[2] = (byte) (x >> 40);
        longByte[3] = (byte) (x >> 32);
        longByte[4] = (byte) (x >> 24);
        longByte[5] = (byte) (x >> 16);
        longByte[6] = (byte) (x >> 8);
        longByte[7] = (byte) (x >> 0);
        return longByte;
    }

    /** 
     * 通过byte数组取到long 
     *  
     * @param bb 
     * @param index 
     * @return 
     */
    public static long getLong(byte[] bb, int index) {
        return ((((long) bb[index + 0] & 0xff) << 56) | (((long) bb[index + 1] & 0xff) << 48) | (((long) bb[index + 2] & 0xff) << 40) | (((long) bb[index + 3] & 0xff) << 32) | (((long) bb[index + 4] & 0xff) << 24) | (((long) bb[index + 5] & 0xff) << 16) | (((long) bb[index + 6] & 0xff) << 8) | (((long) bb[index + 7] & 0xff) << 0));
    }
}
