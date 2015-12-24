package com.hippo.broker.cluster.command;

import java.util.List;

import com.hippo.network.command.Command;

public class RegisterRequest extends Command {
    /**  */
    private static final long serialVersionUID = -5815207601190392165L;
    /**  */
    private String clientId;
    private List<String> bucketsRequired;
    private boolean isUnRegister = false;;

    public RegisterRequest() {
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public List<String> getBucketsRequired() {
        return bucketsRequired;
    }

    public void setBucketsRequired(List<String> bucketsRequired) {
        this.bucketsRequired = bucketsRequired;
    }

    public boolean isUnRegister() {
        return isUnRegister;
    }

    public void setUnRegister(boolean isUnRegister) {
        this.isUnRegister = isUnRegister;
    }

}
