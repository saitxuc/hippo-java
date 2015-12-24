package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public class MdbAddOper extends MdbBaseOper {

    private String sizeFlag;

    private long expireTime;

    private byte[] content;

    private int kLength;

    private int version;

    public MdbAddOper(Integer buckNo, CompleteCallback callback) {
        super(OperEnum.ADD_OPER, buckNo, callback);
    }

    public MdbAddOper(String sizeFlag, long expireTime, byte[] content, int klength, Integer buckNo, int version, CompleteCallback callback) {
        this(buckNo, callback);
        this.sizeFlag = sizeFlag;
        this.expireTime = expireTime;
        this.content = content;
        this.kLength = klength;
        this.version = version;
    }

    public String getSizeFlag() {
        return sizeFlag;
    }

    public void setSizeFlag(String sizeFlag) {
        this.sizeFlag = sizeFlag;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public int getkLength() {
        return kLength;
    }

    public void setkLength(int kLength) {
        this.kLength = kLength;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
