package com.hippo.broker.cluster;

import com.hippo.leveldb.LevelDbMigrationEngine;
import com.hippo.mdb.MdbMigrationEngine;
import com.hippo.store.MigrationEngine;
import com.hippo.store.StoreEngine;

/**
 * @author yangxin
 */
public final class MigrationEngineFactory {
	public static MigrationEngine getMigrationEngine(StoreEngine e) {
		if (e == null) {
			return null;
		}
		
		if ("mdb".equals(e.getName())) {
			return new MdbMigrationEngine(e);
		} else {// levelDb
			return new LevelDbMigrationEngine(e);
		}
	}
}
