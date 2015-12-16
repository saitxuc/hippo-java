package com.pinganfu.hippo.broker.cluster.command;

import com.pinganfu.hippo.network.command.Command;

/**
 * receive List of DB inofs for BucketNo.
 * @author saitxuc
 * 2015-3-6
 */
public class ReplicatedBucketRequest extends Command {
    /**
     * 
     */
    private static final long serialVersionUID = 9192599201584361779L;
    private String buckNo;
    private String sizeFlag;
    private boolean flag = true;
    private String clientId;
    private int port;
    private long modifiedTime;
    private long expiredUpdateTime;

    public ReplicatedBucketRequest() {

    }

    public String getBuckNo() {
        return buckNo;
    }

    public void setBuckNo(String buckNo) {
        this.buckNo = buckNo;
    }

    public boolean getFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getSizeFlag() {
        return sizeFlag;
    }

    public void setSizeFlag(String sizeFlag) {
        this.sizeFlag = sizeFlag;
    }

    public long getExpiredUpdateTime() {
        return expiredUpdateTime;
    }

    public void setExpiredUpdateTime(long expiredUpdateTime) {
        this.expiredUpdateTime = expiredUpdateTime;
    }
}
