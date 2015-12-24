package com.hippo.store.model;

public class GetResult {
    private byte[] content;

    private int version;

    private long expireTime;

    public GetResult() {
        super();
    }
    
    public GetResult(byte[] content, int version) {
        super();
        this.content = content;
        this.version = version;
    }

    public GetResult(byte[] content, int version, long expireTime) {
        super();
        this.content = content;
        this.version = version;
        this.expireTime = expireTime;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
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
