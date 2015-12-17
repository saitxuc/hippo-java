package com.hippo.broker.cluster.simple;

import com.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.hippo.broker.cluster.simple.client.leveldb.BackupClient;
import com.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.hippo.broker.cluster.simple.master.leveldb.BackupServer;
import com.hippo.leveldb.LevelDbMigrationEngine;
import com.hippo.store.MigrationEngine;

public class LevelDbClusterReplicatedFactory implements MSClusterReplicatedFactory {

	@Override
	public IMasterReplicatedServer getReplicatedServe(MigrationEngine migrationEngine, String replicatedPort) {
		return new BackupServer((LevelDbMigrationEngine)migrationEngine, replicatedPort);
	}

	@Override
	public ISlaveReplicatedClient getReplicatedClient(MigrationEngine migrationEngine, ZkRegisterService registerService,
			String replicatedPort) {
		return new BackupClient(((LevelDbMigrationEngine)migrationEngine).getStoreEngine(), registerService, replicatedPort);
	}
	
	@Override
	public String getName() {
		return "levelDb";
	}

}
