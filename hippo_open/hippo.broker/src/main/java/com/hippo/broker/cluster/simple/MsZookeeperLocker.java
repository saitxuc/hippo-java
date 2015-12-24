package com.hippo.broker.cluster.simple;

import java.io.IOException;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.AbstractLocker;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public class MsZookeeperLocker extends AbstractLocker {

    private static final Logger LOG = LoggerFactory.getLogger(MsZookeeperLocker.class);

    private String masterNode = null;

    private ZkClient zkClient;

    private String hostname;

    private final Object sleepMutex = new Object();

    public MsZookeeperLocker(ZkClient zkClient, String masterNode, String hostname) {
        this.zkClient = zkClient;
        this.masterNode = masterNode;
        this.hostname = hostname;
    }

    @Override
    public void doInit() {

    }

    @Override
    public void doStart() {
        /**
         * judge own haven bean master
         */
        while (true) {
            if (this.isStopped()) {
                return;
            }
            if (zkClient != null) {
                boolean exists = zkClient.exists(masterNode + "/result/master");
                if (exists) {
                    String data = zkClient.readData(masterNode + "/result/master");
                    if (data != null) {
                        if (hostname.equalsIgnoreCase(data)) {
                            break;
                        }
                    }
                }
            }
            LOG.info("Failed to acquire lock.  Sleeping for " + lockAcquireSleepInterval + " milli(s) before trying again...");
            try {
                synchronized (sleepMutex) {
                    sleepMutex.wait(lockAcquireSleepInterval);
                }
            } catch (InterruptedException e) {
                LOG.warn("Master lock retry sleep interrupted", e);
                Thread.currentThread().interrupt();
            }

        }
        LOG.info("Becoming the master from Zookeeper, hostname=" + hostname);
    }

    @Override
    public boolean keepAlive() throws IOException {
        boolean result = true;
        try {
            long createtime = zkClient.getCreationTime(masterNode + "/result/master");
            if (createtime == -1) {
                LOG.error("master cannot keep alive zookeeper locker, master server will close. ");
                result = false;
            }
        } catch (Exception e) {
            LOG.error("connection error. master cannot keep alive zookeeper locker, master server will close. ");
            result = false;
        }

        return result;
    }

    @Override
    public void doStop() {
        synchronized (sleepMutex) {
            sleepMutex.notifyAll();
        }
    }

}
