package com.pinganfu.hippo.broker.cluster.simple;

import com.pinganfu.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.pinganfu.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.pinganfu.hippo.store.MigrationEngine;

public interface MSClusterReplicatedFactory {
    public IMasterReplicatedServer getReplicatedServe(MigrationEngine migrationEngine, String replicatedPort);

    public ISlaveReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, ZkRegisterService registerService, String replicatedPort);

    public String getName();
}
