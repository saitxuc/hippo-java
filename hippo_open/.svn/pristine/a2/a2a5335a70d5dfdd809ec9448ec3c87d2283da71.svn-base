package com.pinganfu.hippo.broker.cluster.simple;

import com.pinganfu.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.pinganfu.hippo.broker.cluster.simple.client.leveldb.BackupClient;
import com.pinganfu.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.pinganfu.hippo.broker.cluster.simple.master.leveldb.BackupServer;
import com.pinganfu.hippo.leveldb.LevelDbMigrationEngine;
import com.pinganfu.hippo.store.MigrationEngine;

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
