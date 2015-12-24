package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * Created by Owen on 2015/11/24.
 */
public class MdbBitGetOper extends MdbBaseOper {

    private MdbPointer sInfo;

    private byte[] key = null;

    private int byteIndex;

    private int offsetInByte;

    public MdbBitGetOper(Integer buckNo, CompleteCallback callback) {
        super(OperEnum.BITGET_OPER, buckNo, callback);
    }

    public MdbBitGetOper(Integer buckNo, MdbPointer sInfo, byte[] key, int byteIndex, int offsetInByte, CompleteCallback callback) {
        this(buckNo, callback);
        this.key = key;
        this.sInfo = sInfo;
        this.byteIndex = byteIndex;
        this.offsetInByte = offsetInByte;
    }

    public MdbPointer getsInfo() {
        return sInfo;
    }

    public void setsInfo(MdbPointer sInfo) {
        this.sInfo = sInfo;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getByteIndex() {
        return byteIndex;
    }

    public void setByteIndex(int byteIndex) {
        this.byteIndex = byteIndex;
    }

    public int getOffsetInByte() {
        return offsetInByte;
    }

    public void setOffsetInByte(int offsetInByte) {
        this.offsetInByte = offsetInByte;
    }
}