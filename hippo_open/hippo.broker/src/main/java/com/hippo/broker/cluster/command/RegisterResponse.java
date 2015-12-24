package com.hippo.broker.cluster.command;

import com.hippo.network.command.Response;

public class RegisterResponse extends Response {
    
    /**  */
    private static final long serialVersionUID = 2719359686257575112L;
    
    private String bucketNo;

    public String getBucketNo() {
        return bucketNo;
    }

    public void setBucketNo(String bucketNo) {
        this.bucketNo = bucketNo;
    }
}