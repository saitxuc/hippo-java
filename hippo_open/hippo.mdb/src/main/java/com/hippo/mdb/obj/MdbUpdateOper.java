package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public class MdbUpdateOper extends MdbBaseOper {

    private MdbPointer info;

    private byte[] content;

    private long expiretime;

    private String sizeFlag;

    private int version;

    public MdbUpdateOper(Integer buckNo, CompleteCallback callback) {
        super(OperEnum.UPDATE_OPER, buckNo, callback);
    }

    public MdbUpdateOper(MdbPointer info, Integer buckNo, byte[] content, String sizeFlag, long expiretime, int version, CompleteCallback callback) {
        this(buckNo, callback);
        this.info = info;
        this.content = content;
        this.sizeFlag = sizeFlag;
        this.expiretime = expiretime;
        this.version = version;
    }

    public MdbPointer getInfo() {
        return info;
    }

    public void setInfo(MdbPointer info) {
        this.info = info;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public long getExpiretime() {
        return expiretime;
    }

    public void setExpiretime(long expiretime) {
        this.expiretime = expiretime;
    }

    public String getSizeFlag() {
        return sizeFlag;
    }

    public void setSizeFlag(String sizeFlag) {
        this.sizeFlag = sizeFlag;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
