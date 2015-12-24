package com.hippo.store;

public abstract class TransDataCallBack {

    public abstract void updateModifiedTimeCallBack(int bucket, long dbUpdatedTime);

    public abstract void updateExpiredCallBack(int bucket, long dbUpdatedTime);
}
