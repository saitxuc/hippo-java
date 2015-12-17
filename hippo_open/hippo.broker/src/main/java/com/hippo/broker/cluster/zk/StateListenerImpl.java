package com.hippo.broker.cluster.zk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.cluster.simple.ZkRegisterService;

/**
 * 
 * @author saitxuc
 * 2015-3-27
 */
public class StateListenerImpl implements StateListener {
	private static final Logger LOG = LoggerFactory.getLogger(StateListenerImpl.class);
	
    private ZkRegisterService registerService = null;

    public StateListenerImpl(ZkRegisterService registerService) {
        this.registerService = registerService;
    }

    private final long     MAX_RECONNECT_WAITTIME = 1000;

    private CountDownLatch latch;

    protected Thread       checkConnectThread;

    protected final Object checkpointThreadLock   = new Object();

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        switch (state) {
            case SyncConnected:
                if (latch != null) {
                    latch.countDown();
                }
                break;
            case Disconnected:
                latch = new CountDownLatch(1);
                startCheckpoint();
                break;
            case Expired:
                break;
            default:
                break;
        }
    }

    @Override
    public void handleNewSession() throws Exception {

    }

    public void stopMaster() {
        try {
            registerService.resumeRegister();
        } catch (Exception e) {
            LOG.error(" Master server stop happen error! ");
        }
    }

    private void startCheckpoint() {
        synchronized (checkpointThreadLock) {
            boolean start = false;
            if (checkConnectThread == null) {
                start = true;
            } else if (!checkConnectThread.isAlive()) {
                start = true;
                LOG.info("hippo: Zookeeper connect state checkpoint thread after death");
            }
            if (start) {
                checkConnectThread = new Thread(
                    "Hippo Zookeeper connect state Checkpoint Worker") {
                    @Override
                    public void run() {
                        if (latch != null && latch.getCount() > 0) {
                            try {
                                latch.await(MAX_RECONNECT_WAITTIME, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                            }
                            if (latch.getCount() > 0) {
                                StateListenerImpl.this.stopMaster();
                            } else {
                                latch = null;
                            }
                        }
                    }
                };
                checkConnectThread.setDaemon(true);
                checkConnectThread.start();
            }
        }
    }

    public void stateChanged(int connected) {
    }
}
