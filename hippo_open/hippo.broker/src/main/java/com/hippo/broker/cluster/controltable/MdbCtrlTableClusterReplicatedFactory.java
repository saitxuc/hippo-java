package com.hippo.broker.cluster.controltable;

import java.util.List;

import com.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.hippo.broker.cluster.controltable.master.mdb.MdbCtrlTableReplicatedServer;
import com.hippo.store.MigrationEngine;

public class MdbCtrlTableClusterReplicatedFactory implements ICtrlTableClusterReplicatedFactory {

    @Override
    public ICtrlTableReplicatedServer getReplicatedServe(String replicatedPort, CtrlTableClusterBrokerService broker) {
        return new MdbCtrlTableReplicatedServer(replicatedPort, broker);
    }

    @Override
    public ICtrlTableReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, String masterUrl, List<String> buckets, String clientFlag) {
        return new MdbCtrlTableReplicatedClient(migrationEngine, masterUrl, buckets, clientFlag);
    }

    @Override
    public String getName() {
        return "mdb-cluster";
    }

}
