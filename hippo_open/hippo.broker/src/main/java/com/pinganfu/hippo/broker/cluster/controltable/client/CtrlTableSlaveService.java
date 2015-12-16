package com.pinganfu.hippo.broker.cluster.controltable.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.cluster.controltable.CtrlTableClusterReplicatedFactoryFinder;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableClusterReplicatedFactory;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableReplicatedClient;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.ListUtils;
import com.pinganfu.hippo.store.MigrationEngine;

/**
 * Should be thread safe
 * @author saitxuc
 *
 */
public class CtrlTableSlaveService extends LifeCycleSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(CtrlTableSlaveService.class);

    private Map<String, ICtrlTableReplicatedClient> slaveReplicatedClientMap = new HashMap<String, ICtrlTableReplicatedClient>();
    Map<String, Set<String>> migBucketsMap;
    private MigrationEngine migrationEngine;
    private String clientFlag;
    private String brokername = null;

    public CtrlTableSlaveService(String brokername, MigrationEngine migrationEngine, Map<String, Set<String>> migBucketsMap, String clientFlag) {
        this.brokername = brokername;
        this.migBucketsMap = migBucketsMap;
        this.migrationEngine = migrationEngine;
        this.clientFlag = clientFlag;
    }

    @Override
    public synchronized void doInit() {
        ICtrlTableClusterReplicatedFactory factory = CtrlTableClusterReplicatedFactoryFinder.getClusterReplicatedFactory(migrationEngine.getName());
        Iterator<Entry<String, Set<String>>> iter = migBucketsMap.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Set<String>> entry = iter.next();
            String registerUrl = entry.getKey();
            Set<String> buckets = entry.getValue();
            List<String> bucketsList = ListUtils.setToList(buckets);
            ICtrlTableReplicatedClient client = slaveReplicatedClientMap.get(registerUrl);
            if (null == client) {
                client = factory.getReplicatedClient(migrationEngine, getMasterUrl(registerUrl), bucketsList, clientFlag);
                slaveReplicatedClientMap.put(registerUrl, client);
            }
            client.init();
        }
    }

    @Override
    public synchronized void doStart() {
        Iterator<Entry<String, ICtrlTableReplicatedClient>> clientIter = slaveReplicatedClientMap.entrySet().iterator();
        while (clientIter.hasNext()) {
            ICtrlTableReplicatedClient client = clientIter.next().getValue();
            client.start();
        }
    }

    @Override
    public synchronized void doStop() {
        Iterator<Entry<String, ICtrlTableReplicatedClient>> clientIter = slaveReplicatedClientMap.entrySet().iterator();
        while (clientIter.hasNext()) {
            ICtrlTableReplicatedClient client = clientIter.next().getValue();
            if (client != null) {
                client.stop();
            }
        }
    }

    /**
     * add new migrate buckets, remain old ones
     * @param newMigBucketsMap
     */
    public synchronized void addMigrateBuckets(final Map<String, Set<String>> newMigBucketsMap) {
        LOG.info(" add mig bucket to {} with {}", migBucketsMap, newMigBucketsMap);

        try {
            addMigrateBuckets0(newMigBucketsMap);
        } catch (Exception e) {
            LOG.error(" error when add migrate buckets ", e);
        }

        LOG.info(" replace finished: mig map: {}, master urls: {} ", migBucketsMap, slaveReplicatedClientMap.keySet());
    }

    /**
     * close unused client, add new ones
     * @param remainSlaves
     * @param forceCleanAll: whether to clean all slaves and create brand new ones
     */
    public synchronized void resetSlaves(final Map<String, Set<String>> remainSlaves, boolean forceCleanAll) {
        LOG.info(" resetSlaves: {} with remain slaves: {}, force clean: {} ", migBucketsMap, remainSlaves, forceCleanAll);
        try {
            if (forceCleanAll) {
                closeAll();
            } else {
                closeUnusedClientAndBucket(remainSlaves);
            }

            addMigrateBuckets0(remainSlaves);
        } catch (Exception e) {
            LOG.error(" error when reset slaves ", e);
        }

        LOG.info(" resetSlaves finished, mig buckets map: {} ", migBucketsMap);
    }

    private void closeUnusedClientAndBucket(final Map<String, Set<String>> remainSlaves) {
        // close useless master connection
        final Set<String> remainMasterRegisterUrls = remainSlaves.keySet();
        Iterator<Entry<String, ICtrlTableReplicatedClient>> clientIter = slaveReplicatedClientMap.entrySet().iterator();
        while (clientIter.hasNext()) {
            Entry<String, ICtrlTableReplicatedClient> clientEntry = clientIter.next();
            String registerUrl = clientEntry.getKey();
            ICtrlTableReplicatedClient client = clientEntry.getValue();
            if (!remainMasterRegisterUrls.contains(registerUrl)) {
                clientIter.remove();
                migBucketsMap.remove(registerUrl);

                if (client != null) {
                    // 20150515, for old leveldb
                    // client.resetBuckets(new ArrayList<BucketInfo>());
                    client.stop();
                }
            } else {
                // remove useless buckets from exist connection
                Set<String> remainBuckets = remainSlaves.get(registerUrl);
                resetClient(client, remainBuckets);

                migBucketsMap.put(registerUrl, remainBuckets);
            }
        }
    }

    private void resetClient(ICtrlTableReplicatedClient client, Set<String> buckets) {
        List<BucketInfo> bInfos = new ArrayList<BucketInfo>();
        for (String bucket : buckets) {
            bInfos.add(new BucketInfo(Integer.parseInt(bucket), true));
        }
        client.resetBuckets(bInfos);
    }

    private void addMigrateBuckets0(final Map<String, Set<String>> newMigBucketMap) {
        ICtrlTableClusterReplicatedFactory factory = CtrlTableClusterReplicatedFactoryFinder.getClusterReplicatedFactory(migrationEngine.getName());
        Iterator<Entry<String, Set<String>>> migBucketIter = newMigBucketMap.entrySet().iterator();
        while (migBucketIter.hasNext()) {
            Entry<String, Set<String>> entry = migBucketIter.next();
            String registerUrl = entry.getKey();
            final Set<String> buckets = entry.getValue();
            List<String> bucketsList = ListUtils.setToList(buckets);
            ICtrlTableReplicatedClient client = slaveReplicatedClientMap.get(registerUrl);
            if (null == client) {
                client = factory.getReplicatedClient(migrationEngine, getMasterUrl(registerUrl), bucketsList, clientFlag);
                System.out.println("========start client, bucket: " + bucketsList + " from master: " + registerUrl);
                client.start();

                slaveReplicatedClientMap.put(registerUrl, client);
                migBucketsMap.put(registerUrl, buckets);
            } else {
                // in case: {192.168.1.111:51102:51000=[0]} -> {192.168.1.111:51102:51000=[1]}
                if (buckets != null) {
                    Set<String> migBuckets = new HashSet<String>();
                    migBuckets.addAll(migBucketsMap.get(registerUrl));
                    migBuckets.addAll(buckets);
                    resetClient(client, migBuckets);

                    migBucketsMap.put(registerUrl, migBuckets);
                }
            }
        }
    }

    private void closeAll() {
        Iterator<Entry<String, ICtrlTableReplicatedClient>> iter = slaveReplicatedClientMap.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, ICtrlTableReplicatedClient> client = iter.next();
            client.getValue().stop();
        }
        migBucketsMap.clear();
        slaveReplicatedClientMap.clear();
    }

    private String getMasterUrl(String registerUrl) {
        int eIdx = registerUrl.lastIndexOf(':');
        if (eIdx != -1) {
            return registerUrl.substring(0, eIdx);
        }
        return registerUrl;
    }
}
