package com.pinganfu.hippo.common.domain;

/**
 * 
 * @author saitxuc
 * 2015-4-15
 */
public class HippoClusterConifg implements java.io.Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -5529840816045412810L;

    private String name = null;
    private int copycount;
    private String dbType = null;
    private int replicatePort;
    private int hashcount;
    //bucket limit count for each server
    private String bucketsLimit;

    public HippoClusterConifg() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCopycount() {
        return copycount;
    }

    public void setCopycount(int copycount) {
        this.copycount = copycount;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public int getReplicatePort() {
        return replicatePort;
    }

    public void setReplicatePort(int replicatePort) {
        this.replicatePort = replicatePort;
    }

    public int getHashcount() {
        return hashcount;
    }

    public void setHashcount(int hashcount) {
        this.hashcount = hashcount;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getBucketsLimit() {
        return bucketsLimit;
    }

    public void setBucketsLimit(String bucketsLimit) {
        this.bucketsLimit = bucketsLimit;
    }

}
