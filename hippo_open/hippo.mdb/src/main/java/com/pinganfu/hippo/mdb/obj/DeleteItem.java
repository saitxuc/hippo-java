package com.pinganfu.hippo.mdb.obj;

public class DeleteItem {
    private byte[] key;

    private int offset;

    public DeleteItem(byte[] key, int offset) {
        super();
        this.key = key;
        this.offset = offset;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

}
