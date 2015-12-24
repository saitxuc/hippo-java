package com.hippo.common;

import java.io.Serializable;

public class SyncDataTask implements Serializable{
    /**  */
    private static final long serialVersionUID = 5712265508644131261L;
    private String dbinfoId;
    private String sizeFlag;

    public SyncDataTask() {
        super();
    }

    public SyncDataTask(String dbinfoId, String sizeFlag) {
        this.dbinfoId = dbinfoId;
        this.sizeFlag = sizeFlag;
    }

    public String getDbinfoId() {
        return dbinfoId;
    }

    public void setDbinfoId(String dbinfoId) {
        this.dbinfoId = dbinfoId;
    }

    public String getSizeFlag() {
        return sizeFlag;
    }

    public void setSizeFlag(String sizeFlag) {
        this.sizeFlag = sizeFlag;
    }

}
