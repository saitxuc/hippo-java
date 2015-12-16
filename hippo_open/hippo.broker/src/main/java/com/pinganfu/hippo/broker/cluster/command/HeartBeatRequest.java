package com.pinganfu.hippo.broker.cluster.command;

import java.util.Map;

import com.pinganfu.hippo.network.command.Command;

public class HeartBeatRequest extends Command {
    /**  */
    private static final long serialVersionUID = 6283390131874000655L;
    private String clientId;
    private Map<String, Long> syncTime;
    private Map<String, Long> expireUpdatedTime;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Map<String, Long> getSyncTime() {
        return syncTime;
    }

    public void setSyncTime(Map<String, Long> syncTime) {
        this.syncTime = syncTime;
    }

    public Map<String, Long> getExpireUpdatedTime() {
        return expireUpdatedTime;
    }

    public void setExpireUpdatedTime(Map<String, Long> expireUpdatedTime) {
        this.expireUpdatedTime = expireUpdatedTime;
    }

}
