package com.hippo.redis.paser;

public class CodedType {
    private long length;

    private boolean isCoded;

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public boolean isCoded() {
        return isCoded;
    }

    public void setCoded(boolean isCoded) {
        this.isCoded = isCoded;
    }

}
