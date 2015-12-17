package com.hippo.mdb.obj;

/**
 * @author saitxuc
 */
public class MdbPointer {

    private int buckId;

    private String dbNo;

    private int offset;

    private int length;

    private int kLength;

    private int version;

    public MdbPointer() {

    }

    public MdbPointer(int buckId, String dbNo, int offset, int length, int version) {
        this.buckId = buckId;
        this.dbNo = dbNo;
        this.offset = offset;
        this.length = length;
        this.version = version;
    }

    public String getDbNo() {
        return dbNo;
    }

    public void setDbNo(String dbNo) {
        this.dbNo = dbNo;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getKLength() {
        return kLength;
    }

    public void setKLength(int kLength) {
        this.kLength = kLength;
    }

    public int getBuckId() {
        return buckId;
    }

    public void setBuckId(int buckId) {
        this.buckId = buckId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
