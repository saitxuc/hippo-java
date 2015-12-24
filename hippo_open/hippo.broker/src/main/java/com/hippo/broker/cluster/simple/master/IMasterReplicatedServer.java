package com.hippo.broker.cluster.simple.master;

import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.store.MigrationEngine;

/**
 * @author yangxin
 */
public abstract class IMasterReplicatedServer extends LifeCycleSupport {
	
	/**
	 * 
	 * @param migrationEngine
	 */
	public abstract void setMigerateEngine(MigrationEngine migrationEngine);
	
}
