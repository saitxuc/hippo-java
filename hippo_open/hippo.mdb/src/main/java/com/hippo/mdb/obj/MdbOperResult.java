package com.hippo.mdb.obj;

public class MdbOperResult {
    private byte[] removeKey;
    private byte[] content;
    private int version;
    private MdbPointer mdbPointer;
    private long modifiedTime;
    private long expireTime;
    private String errorCode;

    public byte[] getRemoveKey() {
        return removeKey;
    }

    public void setRemoveKey(byte[] removeKey) {
        this.removeKey = removeKey;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public MdbPointer getMdbPointer() {
        return mdbPointer;
    }

    public void setMdbPointer(MdbPointer mdbPointer) {
        this.mdbPointer = mdbPointer;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

}
