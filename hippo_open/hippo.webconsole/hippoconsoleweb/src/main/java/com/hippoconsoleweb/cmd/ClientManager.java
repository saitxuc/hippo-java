package com.hippoconsoleweb.cmd;

import com.hippo.client.HippoClient;

public class ClientManager {
    private HippoClient client;
    private long createTime;

    public ClientManager(HippoClient client) {
        this.client = client;
        createTime = System.currentTimeMillis();
    }

    public HippoClient getClient() {
        return client;
    }

    public void setClient(HippoClient client) {
        this.client = client;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

}
