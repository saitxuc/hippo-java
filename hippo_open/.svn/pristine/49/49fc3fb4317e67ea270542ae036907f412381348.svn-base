package com.pinganfu.hippo.broker.cluster;

import com.pinganfu.hippo.leveldb.LevelDbMigrationEngine;
import com.pinganfu.hippo.mdb.MdbMigrationEngine;
import com.pinganfu.hippo.store.MigrationEngine;
import com.pinganfu.hippo.store.StoreEngine;

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
