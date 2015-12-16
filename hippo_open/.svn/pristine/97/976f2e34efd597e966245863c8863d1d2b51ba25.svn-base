package com.pinganfu.hippo.mdb;

/**
 * @author saitxuc
 *         write 2014-7-30
 */
public class MdbResult {

    private boolean success = false;

    private byte[] key;

    private byte[] value;

    private int version;

    private String errorCode;

    private long expireTime;

    public MdbResult(byte[] key, boolean success) {
        this.key = key;
        this.success = success;
    }

    public MdbResult(byte[] value, int version) {
        this.success = true;
        this.value = value;
        this.version = version;
    }

    public MdbResult(byte[] key, boolean success, String errorCode) {
        this.success = success;
        this.key = key;
        this.errorCode = errorCode;
    }
    
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
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
