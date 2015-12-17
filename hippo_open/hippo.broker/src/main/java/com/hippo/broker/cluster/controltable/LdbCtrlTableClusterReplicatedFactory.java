package com.hippo.broker.cluster.controltable;

import java.util.List;

import com.hippo.broker.cluster.controltable.client.leveldb.LdbCtrlTableReplicatedClient;
import com.hippo.broker.cluster.controltable.master.leveldb.LdbCtrlTableReplicatedServer;
import com.hippo.leveldb.LevelDbMigrationEngine;
import com.hippo.store.MigrationEngine;

public class LdbCtrlTableClusterReplicatedFactory implements ICtrlTableClusterReplicatedFactory {

    @Override
    public ICtrlTableReplicatedServer getReplicatedServe(String replicatedPort, CtrlTableClusterBrokerService broker) {
        return new LdbCtrlTableReplicatedServer(replicatedPort, broker);
    }

    @Override
    public ICtrlTableReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, String masterUrl, List<String> buckets, String clientFlag) {
    	if (migrationEngine instanceof LevelDbMigrationEngine) {
    		return new LdbCtrlTableReplicatedClient(((LevelDbMigrationEngine)migrationEngine).getStoreEngine(), masterUrl, buckets, clientFlag);
    	}
    	throw new RuntimeException("need levelDb MigrationEngine.");
    }

    @Override
    public String getName() {
        return "levelDb-cluster";
    }

}
