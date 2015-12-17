package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * Created by Owen on 2015/11/23.
 */
public class MdbBitUpdateOper extends MdbBaseOper {
    private MdbPointer info;

    private int offset;

    private int bitSizeLeft;

    private byte[] key;

    private boolean val;

    private int version;

    private int separatorLength;

    private byte[] expire;

    public MdbBitUpdateOper(Integer buckNo, CompleteCallback callback) {
        super(OperEnum.BITUPDATE_OPER, buckNo, callback);
    }

    public MdbBitUpdateOper(MdbPointer info, int offset, byte[] key, int bitSizeLeft, boolean val, int separatorLength, int version, byte[] expire, CompleteCallback callback) {
        this(info.getBuckId(), callback);
        this.offset = offset;
        this.key = key;
        this.info = info;
        this.bitSizeLeft = bitSizeLeft;
        this.val = val;
        this.version = version;
        this.separatorLength = separatorLength;
        this.expire = expire;
    }

    public MdbPointer getInfo() {
        return info;
    }

    public void setInfo(MdbPointer info) {
        this.info = info;
    }

    public int getBitSizeLeft() {
        return bitSizeLeft;
    }

    public void setBitSizeLeft(int bitSizeLeft) {
        this.bitSizeLeft = bitSizeLeft;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public boolean isVal() {
        return val;
    }

    public void setVal(boolean val) {
        this.val = val;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getSeparatorLength() {
        return separatorLength;
    }

    public void setSeparatorLength(int separatorLength) {
        this.separatorLength = separatorLength;
    }

    public byte[] getExpire() {
        return expire;
    }

    public void setExpire(byte[] expire) {
        this.expire = expire;
    }
}


