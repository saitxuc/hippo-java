package com.pinganfu.hippo.redis.paser.util;

import java.util.ArrayList;
import java.util.List;

public class IntSet {
    private final byte[] buff;

    public IntSet(byte[] intsetByte) {
        super();
        this.buff = intsetByte;
    }

    private long decodeEncoding() {
        byte[] encodingbyte = splitByte(buff, 0, 4);
        return ntohl(encodingbyte);
    }

    private long decodeLength() {
        byte[] lengthbyte = splitByte(buff, 4, 4);
        return ntohl(lengthbyte);
    }

    public List<Long> docodeIntsetValue() {
        int encoding = Integer.valueOf(Long.valueOf(decodeEncoding()).toString());
        int length = Integer.valueOf(Long.valueOf(decodeLength()).toString());
        byte[] intsetvalue = new byte[buff.length - 8];
        System.arraycopy(buff, 8, intsetvalue, 0, intsetvalue.length);
        int index = 0;
        List<Long> values = new ArrayList<Long>();
        for (int i = 0; i < length; i++) {
            byte[] val = new byte[encoding];
            System.arraycopy(intsetvalue, index, val, 0, encoding);
            index = (i + 1) * encoding;
            long value = ntohl(val);
            values.add(value);
        }
        return values;
    }

    private long ntohl(byte[] buf) {
        if (buf.length == 8) {
            return (((long) buf[7] & 0x00ff) << 56) + (((long) buf[6] & 0x00ff) << 48) + (((long) buf[5] & 0x00ff) << 40) + (((long) buf[4] & 0x00ff) << 32) + (((long) buf[3] & 0x00ff) << 24) + (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 7) {
            return (((long) buf[6] & 0x00ff) << 48) + (((long) buf[5] & 0x00ff) << 40) + (((long) buf[4] & 0x00ff) << 32) + (((long) buf[3] & 0x00ff) << 24) + (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 6) {
            return (((long) buf[5] & 0x00ff) << 40) + (((long) buf[4] & 0x00ff) << 32) + (((long) buf[3] & 0x00ff) << 24) + (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 5) {
            return (((long) buf[4] & 0x00ff) << 32) + (((long) buf[3] & 0x00ff) << 24) + (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 4) {
            return (((long) buf[3] & 0x00ff) << 24) + (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 3) {
            return (((long) buf[2] & 0x00ff) << 16) + (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else if (buf.length == 2) {
            return (((long) buf[1] & 0x00ff) << 8) + (((long) buf[0] & 0x00ff));
        } else {
            return (((long) buf[0] & 0x00ff));
        }
    }

    public static byte[] splitByte(byte[] buf, int start, int length) {
        byte[] tempbuff = new byte[length];
        int index = 0;
        for (int i = start; i < start + length; i++) {
            tempbuff[index] = buf[i];
            index++;
        }
        return tempbuff;
    }

}
