package com.pinganfu.hippoconsoleweb.cmd;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.client.HippoClient;

public class ClientPool {
    private Logger logger = LoggerFactory.getLogger(ClientPool.class);

    private ConcurrentHashMap<String, ClientManager> pool = new ConcurrentHashMap<String, ClientManager>();

    public HippoClient getClient(String clusterName) {
        ClientManager manager = pool.get(clusterName);
        if (manager != null) {
            return manager.getClient();
        }
        return null;
    }

    public boolean putClientIfAbsent(String clusterName, HippoClient client) {
        ClientManager oldVal = pool.putIfAbsent(clusterName, new ClientManager(client));
        if (oldVal == null) {
            //success
            return true;
        } else {
            //has old value
            return false;
        }
    }
    
    public void removeClient(String clusterName) {
        pool.remove(clusterName);
    }

    public ConcurrentHashMap<String, ClientManager> getPool() {
        return pool;
    }

    public void stopAllClient() {
        for (ClientManager manager : pool.values()) {
            try {
                manager.getClient().stop();
            } catch (Exception e) {
                logger.error("close the hippo client error happened in stopAllClient!!");
            }
        }
    }
}
