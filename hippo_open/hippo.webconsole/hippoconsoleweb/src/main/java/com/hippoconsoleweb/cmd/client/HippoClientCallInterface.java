package com.hippoconsoleweb.cmd.client;

import com.hippo.client.HippoResult;

public interface HippoClientCallInterface {
    public HippoResult get(String key, String clusterName);

    public HippoResult remove(String key, String clusterName);

    public HippoResult inc(String key, String clusterName, String val, String defaultVal, String expireTime);

    public HippoResult decr(String key, String clusterName, String val, String defaultVal, String expireTime);

    public HippoResult set(String key, String clusterName, String val, String expireTime);
    
    public HippoResult sset(String key, String clusterName, String val, String expireTime);
    
    public HippoResult hset(String key, String clusterName, String val, String expireTime);
    
    public HippoResult lset(String key, String clusterName, String val, String expireTime);

    public HippoResult update(String key, String clusterName, String val, String version, String expireTime);
}
