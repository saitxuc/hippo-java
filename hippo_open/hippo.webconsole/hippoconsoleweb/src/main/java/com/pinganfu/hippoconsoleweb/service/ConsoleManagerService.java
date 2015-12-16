package com.pinganfu.hippoconsoleweb.service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Resource;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.ZkConstants;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.NetUtils;
import com.pinganfu.hippoconsoleweb.lisneter.ClusterResultListener;
import com.pinganfu.hippoconsoleweb.lisneter.MigrateInfoChangeListener;
import com.pinganfu.hippoconsoleweb.lisneter.ServerChangeLisneter;
import com.pinganfu.hippoconsoleweb.lisneter.StateListenerImpl;
import com.pinganfu.hippoconsoleweb.util.ZkUtils;
import com.pinganfu.hippoconsoleweb.zk.MsZookeeperLocker;
import com.pinganfu.hippoconsoleweb.zk.ZkRegisterService;

public class ConsoleManagerService extends LifeCycleSupport implements ZkRegisterService {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleManagerService.class);

    private String zkAddress;

    private ZkClient zkClient = null;

    private String consoleNodePath = null;

    private String consoleNodeLockPath = null;

    @Resource
    private BackupService backupService;

    private static Set<String> cluseters = new HashSet<String>();

    // For Master/Slave switch
    private SlaveService sservice;
    private AtomicBoolean ismaster = new AtomicBoolean(false);
    private boolean useLock = true;
    private Locker locker;
    private long lockKeepAlivePeriod = 0;
    private ScheduledThreadPoolExecutor clockDaemon;
    private String hosturl = null;
    private String mySeq = null;
    private String prevLock = null;
    private ClusterResultListener clusterResultListener;
    
    private ConsoleManagerService() {
    }

    @Override
    public void doInit() {
        if (StringUtils.isEmpty(zkAddress)) {
            throw new RuntimeException(" zkAddress is null ");
        }

        consoleNodePath = "/hippo/console";
        consoleNodeLockPath = consoleNodePath + "/lock";
        
        ZkUtils.ensurePathExist(zkAddress, ZkConstants.DEFAULT_PATH_ROOT, CreateMode.PERSISTENT, true);
        ZkUtils.ensurePathExist(zkAddress, consoleNodeLockPath, CreateMode.PERSISTENT, true);
        
        // DPJ
        clusterResultListener = new ClusterResultListener(this);
        
        this.zkClient = ZkUtils.getZKClient(zkAddress);
    }

    @Override
    public void doStart() {
        // DPJ
        this.zkClient.subscribeStateChanges(new StateListenerImpl(this));
        
        preStart();
        initLock();
        
        LOG.info("===========lock acquired");

        subscribeClusterListeners();
    }
    
    private void preStart() {
        registerMight();
        if (this.ismaster.get()) {
            try {
                ZkUtils.createEphemeralPath(this.zkClient, consoleNodePath + "/result/master", hosturl);
            } catch (Exception e) {
                LOG.error("Failed to start console. Reason: " + e, e);
                this.stop();
            }
        } else {
            try {
                //masterUrl is the host name
                String masterUrl = this.getMasterUrl();
                if (!StringUtils.isEmpty(masterUrl)) {
                    this.startSlave();
                } else {
                    this.zkClient.subscribeChildChanges(consoleNodePath + "/result", this.clusterResultListener);
                }

            } catch (Exception e) {
                LOG.error("Failed to start slave job. Reason: " + e, e);
                try {
                    stop();
                } catch (Exception ex) {
                    LOG.warn("Failed to stop console after failure in start. This exception will be ignored.", ex);
                }
                //throw e;
            }
        }
    }
    
    public void startSlave() {
        this.zkClient.unsubscribeChildChanges(consoleNodePath + "/result", this.clusterResultListener);
        this.sservice = new SlaveService(this);
        this.sservice.init();
        this.sservice.start();
    }
    
    private void registerMight() {
        this.hosturl = NetUtils.getLocalHost();
        if (StringUtils.isEmpty(hosturl)) {
            throw new RuntimeException("no ip to register!!");
        }
        
        if (mySeq == null || !zkClient.exists(mySeq)) {
            mySeq = zkClient.createEphemeralSequential(consoleNodeLockPath + "/console_", null);
        }
        prevLock = tryLock();
        if (hasLock()) {
            ismaster.set(true);
        }
    }
    
    private String tryLock() {
        LOG.warn("try Lock [mySeq:" + mySeq + "]");
        int prelen = mySeq.lastIndexOf("_") + 1;
        int myseq = Integer.parseInt(mySeq.substring(prelen));
        List<String> allConsoles = zkClient.getChildren(consoleNodeLockPath);
        int min = Integer.MAX_VALUE;
        int prev = -1;
        String prevStr = null;
        for (String console : allConsoles) {
            int seq = Integer.parseInt(console.substring(console.lastIndexOf("_") + 1));
            if (seq < min) {
                min = seq;
            }
            if (seq > prev && seq < myseq) {
                prev = seq;
                prevStr = console;
            }
        }
        return prevStr;
    }

    public boolean hasLock() {
        if (StringUtils.isEmpty(prevLock)) {
            return true;
        }
        return false;
    }

    @Override
    public void doStop() {
        if(zkClient != null) {
            this.zkClient.unsubscribeAll();
        }
        cluseters.clear();
        // DPJ: must stop locker, otherwise, locker won't doStart when resume
        if(locker != null) {
            locker.stop();
        }
        LOG.info(" console manager closed ");        
    }

    public void initConsole() {
        new Thread(new Runnable() {

            @Override
            public void run() {
                ConsoleManagerService.this.start();                
            }
            
        }, "Console manager thread").start();
    }
    
    private void initLock() {
        if (useLock) {
            if (getLocker() == null) {
                LOG.warn("No locker configured");
            } else {
                getLocker().start();
                if (lockKeepAlivePeriod > 0) {
                    getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            keepLockAlive();
                        }
                    }, lockKeepAlivePeriod, lockKeepAlivePeriod, TimeUnit.MILLISECONDS);
                }
            }
        }
    }
    
    public Locker getLocker() {
        if (locker == null) {
            locker = new MsZookeeperLocker(this.zkClient, consoleNodePath, this.hosturl);
        }
        return locker;
    }
    
    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "Hippo Console Lock KeepAlive Timer");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return clockDaemon;
    }
    
    protected void keepLockAlive() {
        boolean stop = false;
        try {
            Locker locker = getLocker();
            if (locker != null) {
                if (!locker.keepAlive()) {
                    stop = true;
                }
            }
        } catch (IOException e) {
            LOG.warn("locker keepalive resulted in: " + e, e);
        }
        if (stop) {
            stop();
        }
    }

    public void close() {
        this.stop();
    }

    @Override
    public void resumeRegister() {
        LOG.info(" resume register ");
        if (ismaster.get()) {
            // DPJ
            ismaster.set(false);
            stop();
            start();
        } else {
            if (sservice != null) {
                sservice.stop();
            }
            this.registerMight();
            if (!ismaster.get()) {
                String masterUrl = this.getMasterUrl();
                if (!StringUtils.isEmpty(masterUrl)) {
                    this.startSlave();
                } else {
                    this.zkClient.subscribeChildChanges(consoleNodePath + "/result", this.clusterResultListener);
                }
            } else {
                //master url 
                try {
                    ZkUtils.createEphemeralPath(this.zkClient, consoleNodePath + "/result/master", hosturl);
                } catch (Exception e) {
                    LOG.error("Failed to start hippo. Reason: " + e, e);
                    this.stop();
                }
            }
        }
    }

    private void subscribeClusterListeners() {
        List<String> clusterList = ZkUtils.getZKClient(zkAddress).getChildren(ZkConstants.DEFAULT_PATH_ROOT);
        if (clusterList != null && clusterList.size() > 0) {
            for (String clusterName : clusterList) {
                try {
                    subscribeClusterListener(zkAddress, clusterName, new Object());
                } catch(Exception e) {
                    LOG.error(" error when subscribe cluster listener for cluster: " + clusterName, e);
                }
            }
        }
    }

    /**
     * 
     * @param zkAddress
     * @param clusterName
     * @param g_dtableLock: lock ServerChangeLisneter & MigrateInfoChangeListener from updating dtable at the same time
     * @throws Exception
     */
    public void subscribeClusterListener(String zkAddress, String clusterName, Object g_dtableLock) throws Exception {
        LOG.info(" subscribe cluster listener for: {} ", clusterName);
        if (cluseters.contains(clusterName)) {
            LOG.warn(" cluster already exists: {} ", clusterName);
            return;
        }
        cluseters.add(clusterName);

        ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
        String dataserverPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_DATA_SERVERS;
        if (zkClient.exists(dataserverPath)) {
            ServerChangeLisneter scListener = new ServerChangeLisneter(zkAddress, clusterName, g_dtableLock);
            zkClient.subscribeChildChanges(dataserverPath, scListener);
        }

        String migPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_MIGRATION;
        if (zkClient.exists(migPath)) {
            zkClient.subscribeChildChanges(migPath, new MigrateInfoChangeListener(zkAddress, clusterName, backupService, g_dtableLock));
        }
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    @Override
    public String getClusterLockPath() {
        return consoleNodeLockPath + "/" + prevLock;
    }

    @Override
    public boolean exists(String path) {
        return zkClient.exists(path);
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener stateListener) {
        zkClient.subscribeDataChanges(path, stateListener);
    }

    @Override
    public String getMasterUrl() {
        return zkClient.readData(consoleNodePath + "/result/master");
    }

    @Override
    public void subscribeStateChanges(IZkStateListener stateListener) {
        zkClient.subscribeStateChanges(stateListener);
    }

    @Override
    public void unsubscribeData(String url, IZkDataListener listener) {
        zkClient.unsubscribeDataChanges(url, listener);
    }

    @Override
    public void unsubscribeStateChanges(IZkStateListener stateListener) {
        zkClient.unsubscribeStateChanges(stateListener);
    }
}
