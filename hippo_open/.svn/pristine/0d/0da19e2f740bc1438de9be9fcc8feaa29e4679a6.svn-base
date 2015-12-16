package com.pinganfu.hippo.mdb.obj;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * @author saitxuc
 * write 2014-7-28
 */
public class OffsetInfo implements java.io.Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 6414253038433968812L;

    private String dbNo;

    private int offset;

    private long expireTime = -1;

    private ByteBuffer buffer;

    private int kLength = -1;

    private boolean isLru = false;

    //same key has been added multi-times,this flag will be set to true
    private AtomicBoolean recycle = new AtomicBoolean(false);

    public OffsetInfo(String dbNo, int offset, long expiretime, ByteBuffer buffer) {
        this.dbNo = dbNo;
        this.offset = offset;
        this.expireTime = expiretime;
        this.buffer = buffer;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public String getDbNo() {
        return dbNo;
    }

    public void setDbNo(String dbNo) {
        this.dbNo = dbNo;
    }

    public int getKLength() {
        return kLength;
    }

    public void setKLength(int kLength) {
        this.kLength = kLength;
    }

    public boolean isLru() {
        return isLru;
    }

    public void setLru(boolean isLru) {
        this.isLru = isLru;
    }

    public boolean getRecycle() {
        return recycle.get();
    }

    public boolean setRecycle(boolean excepted, boolean newData) {
        return recycle.compareAndSet(excepted, newData);
    }

    public void reset(){
        expireTime = -1;
        kLength = -1;
    }
}
