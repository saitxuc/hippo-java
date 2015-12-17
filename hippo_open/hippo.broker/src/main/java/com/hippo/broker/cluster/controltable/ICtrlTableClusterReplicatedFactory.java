package com.hippo.broker.cluster.controltable;

import java.util.List;

import com.hippo.store.MigrationEngine;

public interface ICtrlTableClusterReplicatedFactory {

    public ICtrlTableReplicatedServer getReplicatedServe(String replicatedPort, CtrlTableClusterBrokerService broker);

    public ICtrlTableReplicatedClient getReplicatedClient(MigrationEngine migrationEngine,  String masterUrl, List<String> buckets, String clientFlag);

    public String getName();
}