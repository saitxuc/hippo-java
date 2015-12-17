package com.hippo.broker.cluster.simple;

import com.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.hippo.broker.cluster.simple.client.mdb.MdbSlaveReplicatedClient;
import com.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.hippo.store.MigrationEngine;

public class MdbClusterReplicatedFactory implements MSClusterReplicatedFactory {

    @Override
    public IMasterReplicatedServer getReplicatedServe(MigrationEngine migrationEngine, String replicatedPort) {
        return new MdbMasterReplicatedServer(migrationEngine, replicatedPort);
    }

    @Override
    public ISlaveReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, ZkRegisterService registerService, String replicatedPort) {
        return new MdbSlaveReplicatedClient(migrationEngine, registerService, replicatedPort);
    }

    @Override
    public String getName() {
        return "mdb";
    }

}
