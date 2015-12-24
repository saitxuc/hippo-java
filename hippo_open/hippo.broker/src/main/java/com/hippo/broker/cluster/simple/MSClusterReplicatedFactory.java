package com.hippo.broker.cluster.simple;

import com.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.hippo.store.MigrationEngine;

public interface MSClusterReplicatedFactory {
    public IMasterReplicatedServer getReplicatedServe(MigrationEngine migrationEngine, String replicatedPort);

    public ISlaveReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, ZkRegisterService registerService, String replicatedPort);

    public String getName();
}
