package com.pinganfu.hippo.broker.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author saitxuc
 *
 */
public class ReplicatedClusterInfo {
	
	private final Map<Integer, ReplicatedMasterInfo> replicateds 
			= new HashMap<Integer, ReplicatedMasterInfo>();
	
	public ReplicatedClusterInfo() {
		super();
	}
	
	public void addReplicatedServerInfo(Integer buckId, ReplicatedMasterInfo info) {
		replicateds.put(buckId, info);
	}
	
	public ReplicatedMasterInfo getReplicatedServerInfo(Integer buckId) {
		return replicateds.get(buckId);
	}

	public Map<Integer, ReplicatedMasterInfo> getReplicateds() {
		return replicateds;
	}
	
}
