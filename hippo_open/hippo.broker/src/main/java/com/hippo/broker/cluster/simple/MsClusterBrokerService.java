package com.hippo.broker.cluster.simple;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.BrokerService;
import com.hippo.broker.Lockable;
import com.hippo.broker.Locker;
import com.hippo.broker.cluster.MigrationEngineFactory;
import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.simple.client.SlaveService;
import com.hippo.broker.cluster.simple.master.MasterService;
import com.hippo.client.ClientConstants;
import com.hippo.client.HippoResult;
import com.hippo.client.util.ZkUtil;
import com.hippo.client.util.ZkUtil.ZKConfig;
import com.hippo.common.util.NetUtils;
import com.hippo.mdb.MdbMigrationEngine;
import com.hippo.network.command.Command;
import com.hippo.store.MigrationEngine;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public class MsClusterBrokerService extends BrokerService implements Lockable, ZkRegisterService {

    private static final Logger LOG = LoggerFactory.getLogger(MsClusterBrokerService.class);

    private static final String DEFAULT_ZK_ROOT = "/hippo-msclusters";

    private String zkUrl = null;

    private String zkPath = DEFAULT_ZK_ROOT;

    private ZkClient zkClient = null;

    private String clusterName = null;

    private String clusterNodePath = null;

    private String clusterNodeLockPath = null;

    private String hosturl = null;

    private String mySeq = null;

    private String prevLock = null;

    private AtomicBoolean ismaster = new AtomicBoolean(false);

    private MasterService mservice;

    private SlaveService sservice;

    private boolean useLock = true;
    private Locker locker;
    long lockKeepAlivePeriod = 0;
    private ScheduledFuture<?> keepAliveTicket;
    private ScheduledThreadPoolExecutor clockDaemon;
    private String replicatedPort = null;

    private ClusterResultListener clusterResultListener;
    
    private MigrationEngine migrationEngine;
    
    private AtomicBoolean isfirstSlave = new AtomicBoolean(false);
    
    public MsClusterBrokerService() {
        super();
    }

    public MsClusterBrokerService(String zkUrl, String clusterName, String replicatedPort) {
        this.zkUrl = zkUrl;
        this.clusterName = clusterName;
        this.replicatedPort = replicatedPort;
    }

    @Override
    public void doInit() {
        super.doInit();
        if (StringUtils.isEmpty(clusterName)) {
            throw new RuntimeException(" cluster name is null. cannot read data from zookeeper. ");
        }
        ZKConfig zkConfig = new ZKConfig(zkUrl);
        LOG.info("msClusterBrokerService zkclient Initialize ");
        this.zkClient = new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, new ZkUtil.StringSerializer());
        this.clusterNodePath = zkPath + "/" + clusterName;
        this.clusterNodeLockPath = clusterNodePath + "/lock";
        this.clusterResultListener = new ClusterResultListener(this);
        migrationEngine = MigrationEngineFactory.getMigrationEngine(this.getCache().getEngine());
    }

    private void initLock() {
        if (useLock) {
            if (getLocker() == null) {
                LOG.warn("No locker configured");
            } else {
                getLocker().start();
                if (lockKeepAlivePeriod > 0) {
                    keepAliveTicket = getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            keepLockAlive();
                        }
                    }, lockKeepAlivePeriod, lockKeepAlivePeriod, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void preStart() {
        registerMight();
        if (this.ismaster.get()) {
            try {
                ZkUtil.createEphemeralPath(this.zkClient, clusterNodePath + "/result/master", hosturl);
                this.mservice = new MasterService(migrationEngine, this, replicatedPort);
                this.mservice.start();
            } catch (Exception e) {
                LOG.error("Failed to start hippo (" + getBrokerName() + "). Reason: " + e, e);
                this.stop();
            }
        } else {
            try {
                startCacheEngine(false);
                //masterUrl is the host name
                String masterUrl = this.getMasterUrl();
                if (!StringUtils.isEmpty(masterUrl)) {
                    this.startSlaveFirst();
                }else{
                	this.zkClient.subscribeChildChanges(clusterNodePath + "/result", this.clusterResultListener);
                }
                
            } catch (Exception e) {
                LOG.error("Failed to start hippo (" + getBrokerName() + "). Reason: " + e, e);
                try {
                    stop();
                } catch (Exception ex) {
                    LOG.warn("Failed to stop broker after failure in start. This exception will be ignored.", ex);
                }
                //throw e;
            }

        }

    }
    
    private void startSlaveFirst() {
    	try{
        	if(this.sservice != null) {
            	this.sservice.stop();
            }
        }catch(Exception e) {
        	//ignore
        }
        //TODO get the bucketNos from console
        //hard code first
        this.sservice = new SlaveService(migrationEngine, this, replicatedPort);
        this.sservice.start();
        if(!isfirstSlave.get()) {
    		this.zkClient.subscribeChildChanges(clusterNodePath + "/result", this.clusterResultListener);
        }
    }
    
    public void startSlave() {
        if (mySeq == null || !zkClient.exists(mySeq)) {
            mySeq = zkClient.createEphemeralSequential(clusterNodeLockPath + "/broker_", null);
        }
        prevLock = tryLock();
        
    	if(isfirstSlave.get()) {
    		this.zkClient.unsubscribeChildChanges(clusterNodePath + "/result", this.clusterResultListener);
        }
    	try{
        	if(this.sservice != null) {
            	this.sservice.stop();
            }
        }catch(Exception e) {
        	//ignore
        }
        //TODO get the bucketNos from console
        //hard code first
        this.sservice = new SlaveService(migrationEngine, this, replicatedPort);
        this.sservice.start();
    }

    @Override
    public void doStart() {
        preStart();
        initLock();
        migrationEngine.start();
        super.doStart();
    }

    @Override
    public void doStop() {
        if (mservice != null) {
            mservice.stop();
        }
        if (sservice != null) {
            sservice.stop();
        }
        this.getLocker().stop();
        migrationEngine.stop();
        super.doStop();
    }

    private void registerMight() {
    	isfirstSlave.set(false);
    	this.hosturl = NetUtils.getLocalHost();
        if (StringUtils.isEmpty(hosturl)) {
            throw new RuntimeException("no ip to register!!");
        }
        if (!zkClient.exists(zkPath)) {
            zkClient.create(zkPath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodePath)) {
            zkClient.create(clusterNodePath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeLockPath)) {
            zkClient.create(clusterNodeLockPath, "", CreateMode.PERSISTENT);
        }

        if (!zkClient.exists(clusterNodePath + "/result")) {
            zkClient.create((clusterNodePath + "/result"), "", CreateMode.PERSISTENT);
        }

        if (mySeq == null || !zkClient.exists(mySeq)) {
            mySeq = zkClient.createEphemeralSequential(clusterNodeLockPath + "/broker_", null);
        }
        prevLock = tryLock();
        if (hasLock()) {
            ismaster.set(true);
        }
    }
    
    public String getZkUrl() {
        return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    public String getZkPath() {
        return zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public AtomicBoolean getIsmaster() {
        return ismaster;
    }

    public void setIsmaster(AtomicBoolean ismaster) {
        this.ismaster = ismaster;
    }

    private String tryLock() {
        LOG.warn("try Lock [mySeq:" + mySeq + "]");
        int prelen = mySeq.lastIndexOf("_") + 1;
        int myseq = Integer.parseInt(mySeq.substring(prelen));
        List<String> allBroker = zkClient.getChildren(clusterNodeLockPath);
        int min = Integer.MAX_VALUE;
        int prev = -1;
        String prevStr = null;
        for (String broker : allBroker) {
            int seq = Integer.parseInt(broker.substring(broker.lastIndexOf("_") + 1));
            if (seq < min) {
                min = seq;
            }
            if (seq > prev && seq < myseq) {
                prev = seq;
                prevStr = broker;
            }
        }
        if(min == prev) {
        	isfirstSlave.set(true);
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
    public void setUseLock(boolean useLock) {
        this.useLock = useLock;
    }

    @Override
    public void setLocker(Locker locker) throws IOException {
        this.locker = locker;
    }

    @Override
    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod) {
        this.lockKeepAlivePeriod = lockKeepAlivePeriod;
    }

    public Locker getLocker() {
        if (locker == null) {
            locker = new MsZookeeperLocker(this.zkClient, this.clusterNodePath, this.hosturl);
        }
        return locker;
    }

    @Override
    public HippoResult processCommand(Command command) {
        String bucketNo = command.getHeaders().get(ClientConstants.HEAD_BUCKET_NO);
        if (StringUtils.isEmpty(bucketNo)) {
            command.getHeaders().put(ClientConstants.HEAD_BUCKET_NO, ReplicatedConstants.DEFAULT_BUCKET_NO);
        }
        return super.processCommand(command);
    }

    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ActiveMQ Lock KeepAlive Timer");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return clockDaemon;
    }

    public void resumeRegister() {
        if (ismaster.get()) {
        	ismaster.set(false);
        	stop();
            start();
        } else {
            if (sservice != null) {
                sservice.stop();
            }
            this.registerMight();
            if (ismaster.get()) {
            	//master url 
                try {
                    this.mservice = new MasterService(migrationEngine, this, replicatedPort);
                    //this.mservice.init();
                    this.mservice.start();
                    ZkUtil.createEphemeralPath(this.zkClient, clusterNodePath + "/result/master", hosturl);
                } catch (Exception e) {
                    LOG.error("Failed to start hippo (" + getBrokerName() + "). Reason: " + e, e);
                    this.stop();
                } 
            } 
        }

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

    @Override
    public String getClusterLockPath() {
        return clusterNodeLockPath + "/" + prevLock;
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
        return zkClient.readData(clusterNodePath + "/result/master");
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
