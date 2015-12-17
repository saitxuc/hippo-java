package com.hippo.broker.cluster.command;

import java.util.List;
import java.util.Map;

import com.hippo.network.command.Response;

/**
 * 
 * @author saitxuc
 * 2015-3-9
 */
public class ReplicatedBucketResponse extends Response {

    /**
     * 
     */
    private static final long serialVersionUID = -4205495080513055572L;

    private String bucketNo;

    private Map<String, List<String>> dbinfos;

    private long modifiedTime;

    public ReplicatedBucketResponse() {

    }

    public String getBucketNo() {
        return bucketNo;
    }

    public void setBucketNo(String bucketNo) {
        this.bucketNo = bucketNo;
    }

    public Map<String, List<String>> getDbinfos() {
        return dbinfos;
    }

    public void setDbinfos(Map<String, List<String>> dbinfos) {
        this.dbinfos = dbinfos;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

}
