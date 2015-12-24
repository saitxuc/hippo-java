package com.hippo.mdb.obj;

/**
 * @author saitxuc
 */
public class FetchBean {

    private byte[] originData;

    private int kLength;

    public FetchBean(byte[] originData, int kLength) {
        this.originData = originData;
        this.kLength = kLength;
    }

    public byte[] getOriginData() {
        return originData;
    }

    public int getKLength() {
        return kLength;
    }
}
