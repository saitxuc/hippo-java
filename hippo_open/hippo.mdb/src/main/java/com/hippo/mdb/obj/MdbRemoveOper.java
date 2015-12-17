package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public class MdbRemoveOper extends MdbBaseOper {

    private MdbPointer info;

    private byte[] key;

    private int version;

    public MdbRemoveOper(Integer buckNo, int version, CompleteCallback callback, byte[] key) {
        super(OperEnum.REMOVE_OPER, buckNo, callback);
        this.key = key;
        this.version = version;
    }

    public MdbRemoveOper(MdbPointer info, Integer buckNo, int version, CompleteCallback callback, byte[] key) {
        this(buckNo, version, callback, key);
        this.info = info;
    }

    public MdbPointer getInfo() {
        return info;
    }

    public void setInfo(MdbPointer info) {
        this.info = info;
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

}
