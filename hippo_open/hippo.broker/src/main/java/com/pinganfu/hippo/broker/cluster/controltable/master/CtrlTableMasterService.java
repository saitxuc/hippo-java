package com.pinganfu.hippo.broker.cluster.controltable.master;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.cluster.controltable.CtrlTableClusterBrokerService;
import com.pinganfu.hippo.broker.cluster.controltable.CtrlTableClusterReplicatedFactoryFinder;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableClusterReplicatedFactory;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableReplicatedServer;
import com.pinganfu.hippo.broker.cluster.controltable.client.CtrlTableSlaveService;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.domain.HippoClusterMigrateInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.ListUtils;
import com.pinganfu.hippo.store.MigrationEngine;

/**
 * 
 * @author saitxuc
 *
 */
public class CtrlTableMasterService extends LifeCycleSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(CtrlTableMasterService.class);

    private ICtrlTableReplicatedServer masterReplicatedServer;

    public CtrlTableMasterService(String dbtype, String replicatedPort, CtrlTableClusterBrokerService broker) {
        ICtrlTableClusterReplicatedFactory factory = CtrlTableClusterReplicatedFactoryFinder.getClusterReplicatedFactory(dbtype);
        this.masterReplicatedServer = factory.getReplicatedServe(replicatedPort,broker);
    }

    @Override
    public void doInit() {
        masterReplicatedServer.init();
    }

    @Override
    public void doStart() {
        masterReplicatedServer.start();
    }

    @Override
    public void doStop() {
        masterReplicatedServer.stop();
    }
    
    @SuppressWarnings("unchecked")
    public synchronized void resetBuckets(Set<BucketInfo> resetBuckets, boolean clearTriggerReplicatedEvent) {
        LOG.info(" resetMaster: {}, clearTriggerReplicatedEvent: {} ", resetBuckets, clearTriggerReplicatedEvent);
        masterReplicatedServer.resetBuckets(ListUtils.setToList(resetBuckets), clearTriggerReplicatedEvent);
    }
    
    public void setMigrationEngine(MigrationEngine migrationEngine) {
    	if(masterReplicatedServer != null) {
    		masterReplicatedServer.setMigerateEngine(migrationEngine);
    	}
    }
}
