package com.hippo.broker.cluster.controltable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import com.hippo.common.config.PropConfigConstants;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import com.hippo.broker.BrokerService;
import com.hippo.broker.DefaultCache;
import com.hippo.broker.cluster.MigrationEngineFactory;
import com.hippo.broker.cluster.controltable.client.CtrlTableSlaveService;
import com.hippo.broker.cluster.controltable.master.CtrlTableMasterService;
import com.hippo.broker.cluster.zk.CtrlTableStateListenerImpl;
import com.hippo.broker.cluster.zk.DtableChangeListener;
import com.hippo.broker.cluster.zk.MtableChangeListener;
import com.hippo.client.util.ZkUtil;
import com.hippo.client.util.ZkUtil.ZKConfig;
import com.hippo.common.ZkConstants;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.domain.HippoClusterConifg;
import com.hippo.common.domain.HippoClusterMigrateInfo;
import com.hippo.common.domain.HippoClusterTableInfo;
import com.hippo.common.util.FastjsonUtil;
import com.hippo.common.util.LimitUtils;
import com.hippo.common.util.ListUtils;
import com.hippo.common.util.ServerTableUtil;
import com.hippo.store.MigrationEngine;
import com.hippo.store.StoreEngine;
import com.hippo.store.StoreEngineFactory;

/**
 * 
 * @author saitxuc
 * 2015-4-15
 */
public class CtrlTableClusterBrokerService extends BrokerService implements CtrlTableChangeManager, CtrlTableZkRegisterService {

    private static final Logger LOG = LoggerFactory.getLogger(CtrlTableClusterBrokerService.class);

    private static final String DEFAULT_ZK_ROOT = ZkConstants.NODE_HIPPO;

    private String zkPath = ZkConstants.DEFAULT_PATH_ROOT;

    private String zkUrl = null;

    private String clusterName = null;

    private ZkClient zkClient = null;

    private String clusterNodePath = null;

    private String clusterNodeConfigPath = null;

    private String clusterNodeTablesPath = null;

    private String clusterNodeDtablePath = null;

    private String clusterNodeMtablePath = null;

    private String clusterNodeCtablePath = null;

    private String clusterNodeMigrationPath = null;

    private String clusterNodeDataserversPath = null;

    private String hostip = null;
    /** registerUrl = hostip:replicated_port:service_port */
    private String registerUrl = null;

    private IZkDataListener mtableChangeListner = null;
    private IZkDataListener dtableChangeListner = null;

    private HippoClusterConifg config = null;

    private AtomicBoolean dtableCreated = new AtomicBoolean(false);
    private final Object waitDtableMutex = new Object();

    private AtomicBoolean migFinished = new AtomicBoolean(false);
    private final Object waitMigMutex = new Object();

    private AtomicBoolean serviceInited = new AtomicBoolean(false);

    private Set<BucketInfo> masterBuckets = new HashSet<BucketInfo>();
    private Set<BucketInfo> slaveBuckets = new HashSet<BucketInfo>();
    private Set<BucketInfo> myOwnedBuckets = new HashSet<BucketInfo>();

    private CtrlTableMasterService mservice;
    private CtrlTableSlaveService sservice;

    private String servicePort = null;
    private String replicatedPort = null;

    /** key: master url; value: buckets which I'm slaving */
    private Map<String, Set<String>> master_slavingBuckets_map;

    private MigrationEngine migrationEngine;

    private Map<String, String> localProps = null;

    /** key: bucket; value: slave set need migrate from me */
    private ConcurrentMap<Integer, Set<String>> needMig_bucket_slave_map = new ConcurrentHashMap<Integer, Set<String>>();
    private ConcurrentMap<Integer, Set<String>> needMig_bucket_slave_map_bak = new ConcurrentHashMap<Integer, Set<String>>();

    public CtrlTableClusterBrokerService(String zkUrl, String clusterName, String replicatedPort, Map<String, String> localProps) {
        this(zkUrl, clusterName, null, null, replicatedPort, localProps);
    }

    public CtrlTableClusterBrokerService(String zkUrl, String clusterName, String brokerName, String brokerUris, String replicatedPort, Map<String, String> localProps) {
        this.zkUrl = zkUrl;
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerUris = brokerUris;
        this.replicatedPort = replicatedPort;
        this.localProps = localProps;
    }

    @Override
    public void doInit() {
        if (StringUtils.isEmpty(clusterName)) {
            throw new RuntimeException(" cluster name is null. cannot read data from zookeeper. ");
        }
        ZKConfig zkConfig = new ZKConfig(zkUrl);
        if (LOG.isInfoEnabled()) {
            LOG.info(" ControlTableClusterBrokerService zkclient Initialize ");
        }
        this.zkClient = new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, new ZkUtil.StringSerializer());

        this.clusterNodePath = zkPath + "/" + clusterName;
        this.clusterNodeConfigPath = clusterNodePath + ZkConstants.NODE_CONFIG;
        this.clusterNodeTablesPath = clusterNodePath + ZkConstants.NODE_TABLES;
        this.clusterNodeDataserversPath = clusterNodePath + ZkConstants.NODE_DATA_SERVERS;
        this.clusterNodeDtablePath = clusterNodeTablesPath + ZkConstants.NODE_DTABLE;
        this.clusterNodeMtablePath = clusterNodeTablesPath + ZkConstants.NODE_MTABLE;
        this.clusterNodeCtablePath = clusterNodeTablesPath + ZkConstants.NODE_CTABLE;
        this.clusterNodeMigrationPath = clusterNodePath + ZkConstants.NODE_MIGRATION;

        initZkInfo();

        initConfig();

        this.mservice = new CtrlTableMasterService(config.getDbType(), replicatedPort, this);
        this.mservice.start();

        zkClient.subscribeDataChanges(clusterNodeDtablePath, dtableChangeListner);
        zkClient.subscribeDataChanges(clusterNodeMtablePath, mtableChangeListner);
        zkClient.subscribeStateChanges(new CtrlTableStateListenerImpl(this));

        waitForMigrateFinishByInit();

        registerDataServer();

        waitForDtableCreatedByInit();

        initDbEngineForClusterConfig();

        super.doInit();

        String dtableInfoStr = zkClient.readData(clusterNodeDtablePath);
        HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
        String mtableInfoStr = zkClient.readData(clusterNodeMtablePath);
        HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);

        this.master_slavingBuckets_map = ServerTableUtil.getNewMigrateBucketMapOfServer(registerUrl, mtableInfo.getTableMap(), dtableInfo.getTableMap());

        this.sservice = new CtrlTableSlaveService(this.getBrokerName(), migrationEngine, master_slavingBuckets_map, registerUrl);
    }

    @Override
    public void doStart() {
        super.doStart();
        if (migrationEngine != null) {
            migrationEngine.start();
        }
        if(sservice != null) {
            this.sservice.start();
        }

        this.serviceInited.compareAndSet(false, true);
        
        LOG.info(" dtable change might in doStart ");
        dtableChangeMight();
    }

    @Override
    public void doStop() {
        if (mservice != null) {
            mservice.stop();
        }
        if (sservice != null) {
            sservice.stop();
        }
        if (migrationEngine != null) {
            migrationEngine.stop();
        }
        unRegisterDataServer();
        super.doStop();
    }

    private void registerDataServer() {
        LOG.info(" register data server: {}", registerUrl);
        String registerPath = clusterNodeDataserversPath + "/" + registerUrl;
        if (!zkClient.exists(registerPath)) {
            zkClient.createEphemeral(registerPath);
        }
    }
    
    private void unRegisterDataServer() {
    	String registerPath = clusterNodeDataserversPath + "/" + registerUrl;
    	zkClient.delete(registerPath);
    }
    
    @SuppressWarnings("unchecked")
    private void initDbEngineForClusterConfig() {
        LOG.info("initDbEngineForClusterConfig");
        String dbType = config.getDbType();
        String bucketCount = config.getBucketsLimit();
        StoreEngine storeEngine = StoreEngineFactory.findStoreEngine(dbType);
        storeEngine.setBuckets(ListUtils.setToList(myOwnedBuckets));

        this.cache = new DefaultCache(this, storeEngine);
        cache.setInitParams(localProps);

        if (!StringUtils.isEmpty(bucketCount)) {
            int bucketLimit4Single = Integer.parseInt(bucketCount);
            if (bucketLimit4Single == -1) {
                throw new RuntimeException(" bucket limit set Illegal, please check! ");
            }
            this.cache.setBucketLimit(bucketLimit4Single);
        }
        loadLocalProps();
        migrationEngine = MigrationEngineFactory.getMigrationEngine(this.getCache().getEngine());
        mservice.setMigrationEngine(migrationEngine);
    }

    private void loadLocalProps() {
        String capacityLimit = this.localProps.get(PropConfigConstants.DB_LIMIT);
        if (!StringUtils.isEmpty(capacityLimit)) {
            long capacityLimitNum = LimitUtils.calculationLimit(capacityLimit);
            if (capacityLimitNum == -1) {
                throw new RuntimeException(" db limit set Illegal, please check! ");
            }
            this.setLimit(capacityLimitNum);
        }
    }

    @Override
    public synchronized void handleDtableDataChange(String dtableInfoStr) {
        LOG.info(" handle dtable changed ");

        String mtableInfoStr = zkClient.readData(clusterNodeMtablePath);
        HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
        HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);

        checkInited(mtableInfo, dtableInfo);

        if (migFinished.get() && dtableCreated.get()) {
            if (!this.serviceInited.get()) {
                return;
            }

            dtableChangeMight();
        }
    }

    private synchronized void dtableChangeMight() {
        String dtableInfoStr = zkClient.readData(clusterNodeDtablePath);
        HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
        String mtableInfoStr = zkClient.readData(clusterNodeMtablePath);
        HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);

        if (null == dtableInfo || null == mtableInfo) {
            return;
        }

        Map<Integer, Vector<String>> mtable = mtableInfo.getTableMap();
        Map<Integer, Vector<String>> dtable = dtableInfo.getTableMap();
        if (null == mtable || null == dtable) {
            return;
        }

        // if only version changed
        if (mtable.toString().equals(dtable.toString())) {
            return;
        }
        
        LOG.info(" dtable changed, mtable: {} ", mtableInfoStr);
        LOG.info(" dtable changed, dtable: {} ", dtableInfoStr);

        this.resetMyOwnedBuckets(dtable);
        // add mtable first line
        Set<BucketInfo> mBuckets = new HashSet<BucketInfo>();
        mBuckets.addAll(myOwnedBuckets);
        Vector<String> mtableFirstLine = mtable.get(0);
        for (int i = 0; i < mtableFirstLine.size(); i++) {
            if (registerUrl.equals(mtableFirstLine.get(i))) {
                mBuckets.add(new BucketInfo(i, false));
            }
        }
        this.mservice.resetBuckets(mBuckets, true); // buckets may has conflict BucketInfo: (1,false) vs. (1,true), it's ok

        this.master_slavingBuckets_map = ServerTableUtil.getNewMigrateBucketMapOfServer(registerUrl, mtable, dtable);
        this.sservice.resetSlaves(master_slavingBuckets_map, false);

        // reset migrateBucket_master_map, migrateBucket_master_map_bak
        synchronized(needMig_bucket_slave_map) {
            needMig_bucket_slave_map.clear();
            needMig_bucket_slave_map_bak.clear();
            Iterator<BucketInfo> mBucketsIter = mBuckets.iterator();
            while (mBucketsIter.hasNext()) {
                int bucket = mBucketsIter.next().getBucketNo();
                
                Set<String> needMigSlave = ServerTableUtil.getNeedMigrateSlaves(bucket, mtable, dtable);

                needMig_bucket_slave_map.put(bucket, needMigSlave);
                needMig_bucket_slave_map_bak.put(bucket, ListUtils.copySet(needMigSlave));
            }
            
            recheckIfMigrateFinished();
        }
        
        LOG.info(" reset m/sservice buckets on dtable change, m buckets: {}, master_slavingBuckets_map: {} ", mBuckets, master_slavingBuckets_map);
    }

    @Override
    public synchronized void handleMtableDataChange(String mtableInfoStr) {
        LOG.info(" handle mtable changed ");

        String dtableInfoStr = zkClient.readData(clusterNodeDtablePath);
        HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
        HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);

        checkInited(mtableInfo, dtableInfo);

        if (null == dtableInfo || null == mtableInfo) {
            return;
        }

        Map<Integer, Vector<String>> mtable = mtableInfo.getTableMap();

        if (migFinished.get() && dtableCreated.get() && mtableInfoStr.equals(dtableInfoStr)) {
            LOG.info(" mtable changed, mtable = dtable = {} ", mtableInfoStr);
            
            Map<String, Set<String>> remainSlaves = ServerTableUtil.getSlaveBucketMapOfServer(registerUrl, mtable);
            this.sservice.resetSlaves(remainSlaves, false);
            LOG.info(" reset sservice buckets on mig finished: " + remainSlaves);

            this.resetMyOwnedBuckets(dtableInfo.getTableMap());
            // TODO: check
            this.mservice.resetBuckets(myOwnedBuckets, false);
            LOG.info(" reset mservice buckets on mig finished: " + myOwnedBuckets);
        }
        //or do nothing
    }

    private void checkInited(HippoClusterTableInfo mtableInfo, HippoClusterTableInfo dtableInfo) {
        if (null == this.cache || null == this.cache.getEngine()) {
            LOG.info(" cache not inited ");
            if (mtableInfo != null && dtableInfo != null && mtableInfo.getVersion() == dtableInfo.getVersion()) {
                if (migFinished.compareAndSet(false, true)) {
                    synchronized (waitMigMutex) {
                        this.waitMigMutex.notifyAll();
                    }
                }
            }

            if (!ServerTableUtil.isEmptyTable(dtableInfo.getTableMap())) {
                if (dtableCreated.compareAndSet(false, true)) {
                    synchronized (waitDtableMutex) {
                        this.waitDtableMutex.notifyAll();
                    }
                }
            }
        }
    }

    private void initZkInfo() {
        if (!zkClient.exists(DEFAULT_ZK_ROOT)) {
            zkClient.create(DEFAULT_ZK_ROOT, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(zkPath)) {
            zkClient.create(zkPath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodePath)) {
            zkClient.create(clusterNodePath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeConfigPath)) {
            zkClient.create(clusterNodeConfigPath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeTablesPath)) {
            zkClient.create(clusterNodeTablesPath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeCtablePath)) {
            zkClient.create(clusterNodeCtablePath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeDtablePath)) {
            zkClient.create(clusterNodeDtablePath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeMtablePath)) {
            zkClient.create(clusterNodeMtablePath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeDataserversPath)) {
            zkClient.create(clusterNodeDataserversPath, "", CreateMode.PERSISTENT);
        }
        if (!zkClient.exists(clusterNodeMigrationPath)) {
            zkClient.create(clusterNodeMigrationPath, "", CreateMode.PERSISTENT);
        }
        this.dtableChangeListner = new DtableChangeListener(this);
        this.mtableChangeListner = new MtableChangeListener(this);
    }

    private void initConfig() {
        String cconfigString = zkClient.readData(clusterNodeConfigPath);
        config = FastjsonUtil.jsonToObj(cconfigString, HippoClusterConifg.class);
        if (null == config) {
            throw new RuntimeException(" install broker happen error: can't get config from zk");
        }

        int idx = this.brokerUris.lastIndexOf(':');
        if (this.brokerUris.indexOf('/') != -1 && idx != -1) {
            this.hostip = this.brokerUris.substring(this.brokerUris.lastIndexOf('/') + 1, idx);
            this.servicePort = this.brokerUris.substring(idx + 1);
            this.registerUrl = this.hostip + ":" + this.replicatedPort + ":" + this.servicePort;
        }

        if (StringUtils.isEmpty(registerUrl)) {
            throw new RuntimeException("no ip to register!");
        }
    }

    private void resetMyOwnedBuckets(Map<Integer, Vector<String>> table) {
        this.masterBuckets.clear();
        this.masterBuckets = ServerTableUtil.getMyMasterBuckets(this.registerUrl, table);
        this.slaveBuckets.clear();
        this.slaveBuckets = ServerTableUtil.getMySlaveBuckets(this.registerUrl, table);

        this.myOwnedBuckets.clear();
        this.myOwnedBuckets.addAll(masterBuckets);
        this.myOwnedBuckets.addAll(slaveBuckets);

        LOG.info(" resetMyOwnedBuckets complete: master buckets: " + masterBuckets);
        LOG.info(" resetMyOwnedBuckets complete: slave buckets: " + slaveBuckets);
    }

    private void waitForDtableCreatedByInit() {
        LOG.info(" wait for dtable to be created... ");
        while (true) {
            if (this.isStopped()) {
                return;
            }
            if (dtableCreated.get()) {
                break;
            }
            try {
                synchronized (waitDtableMutex) {
                    waitDtableMutex.wait(10 * 1000);
                }
            } catch (InterruptedException e) {
                LOG.warn(" wait for dtable created sleep interrupted", e);
                Thread.currentThread().interrupt();
            }

        }
        LOG.info(" dtable created, init broker. ");
    }

    private void waitForMigrateFinishByInit() {
        String dtableInfoStr = zkClient.readData(clusterNodeDtablePath);
        HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
        if (null == dtableInfo) {
            migFinished.compareAndSet(false, true);
            return;
        } else {
            String mtableInfoStr = zkClient.readData(clusterNodeMtablePath);
            HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);
            if (mtableInfo != null && mtableInfo.getVersion() == dtableInfo.getVersion()) {
                migFinished.compareAndSet(false, true);
                return;
            }
        }

        while (true) {
            if (this.isStopped()) {
                return;
            }
            if (migFinished.get()) {
                break;
            }
            try {
                synchronized (waitMigMutex) {
                    waitMigMutex.wait(10 * 1000);
                }
            } catch (InterruptedException e) {
                LOG.warn("Master lock retry sleep interrupted", e);
                Thread.currentThread().interrupt();
            }

        }
        LOG.info(" Migrate finished, mt.version = dt.version. ");
    }

    private ConcurrentMap<Integer, Set<String>> migFinished_bucket_slave_map = new ConcurrentHashMap<Integer, Set<String>>();
    
    public void whenMigrateBucketFinishedCallback(int bucket, String clientId) {
        LOG.info(" migrate bucket finished callback, bucket: {}, clietnId: {} ", bucket, clientId);
        LOG.info(" migrate bucket finished callback, migrateBucket_master_map: {} ", needMig_bucket_slave_map);
        
        Set<String> migFinishedSlave = migFinished_bucket_slave_map.get(bucket);
        if(null == migFinishedSlave) {
            migFinishedSlave = new HashSet<String>();
            migFinished_bucket_slave_map.put(bucket, migFinishedSlave);
        }
        migFinishedSlave.add(clientId);
        
        synchronized(needMig_bucket_slave_map) {
            Set<String> needMigSlaves = needMig_bucket_slave_map.get(bucket);
            if (needMigSlaves != null) {
                LOG.info(" needMigSlaves: {}, finishedClients: {} ", needMigSlaves, migFinishedSlave);
                if(migFinishedSlave.containsAll(needMigSlaves)) {
                    migFinished_bucket_slave_map.remove(bucket);
                    needMig_bucket_slave_map.remove(bucket);
                    
                    //migFinishedSlave.clear();
                    //needMigSlaves.clear();

                    HippoClusterMigrateInfo migrateInfoBean = new HippoClusterMigrateInfo();
                    migrateInfoBean.setBucket(bucket);
                    migrateInfoBean.setOkServers(needMig_bucket_slave_map_bak.get(bucket));

                    // DPJ: 20150616
                    if(migrateInfoBean.getOkServers() != null && migrateInfoBean.getOkServers().size() > 0) {
                        LOG.info("*** mig update zk ");
                        String path = clusterNodeMigrationPath + "/" + bucket;
                        if (zkClient.exists(path)) {
                            zkClient.writeData(path, FastjsonUtil.objToJson(migrateInfoBean));
                        } else {
                            zkClient.createPersistent(path, FastjsonUtil.objToJson(migrateInfoBean));
                        }
                    }
                }
            }
        }
    }
    
    private void recheckIfMigrateFinished() {
        LOG.info(" recheckIfMigrateFinished: need mig: {}, finished: {} ", needMig_bucket_slave_map, migFinished_bucket_slave_map);
        Iterator<Entry<Integer, Set<String>>> needMigIter = needMig_bucket_slave_map.entrySet().iterator();
        while(needMigIter.hasNext()) {
            Entry<Integer, Set<String>> needMigEntry = needMigIter.next();
            int bucket = needMigEntry.getKey();
            Set<String> needMigSlaves = needMigEntry.getValue();
            Set<String> migFinishedSlave = migFinished_bucket_slave_map.get(bucket);
            
            if(migFinishedSlave != null) {
                if(migFinishedSlave.containsAll(needMigSlaves)) {
                    LOG.info("*** mig update zk in reCheckMigFinished ");
                    migFinished_bucket_slave_map.remove(bucket);
                    needMigIter.remove();
                    
                    //migFinishedSlave.clear();
                    //needMigSlaves.clear();
                    
                    HippoClusterMigrateInfo migrateInfoBean = new HippoClusterMigrateInfo();
                    migrateInfoBean.setBucket(bucket);
                    migrateInfoBean.setOkServers(needMig_bucket_slave_map_bak.get(bucket));

                    String path = clusterNodeMigrationPath + "/" + bucket;
                    if (zkClient.exists(path)) {
                        zkClient.writeData(path, FastjsonUtil.objToJson(migrateInfoBean));
                    } else {
                        zkClient.createPersistent(path, FastjsonUtil.objToJson(migrateInfoBean));
                    }
                }
            }
        }
    }

    @Override
    public void reconnectCallback() {
        LOG.info(" reconnect callback ");
        this.registerDataServer();
    }

    @Override
    public void disconnectCallback() {
        LOG.info(" disconnect callback ");
        // do nothing
    }

}
