package com.pinganfu.hippo.common.domain;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * @author DPJ
 */
public class HippoClusterMigrateInfo implements java.io.Serializable {

	
	/**  */
    private static final long serialVersionUID = 8288318536887010485L;
    
	private int bucket = -1;
	private Set<String> okServers = new HashSet<String>();
	private Set<String> failServers = new HashSet<String>();
	
	public HippoClusterMigrateInfo() {
		
	}

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public Set<String> getOkServers() {
        return okServers;
    }

    public void setOkServers(Set<String> okServers) {
        this.okServers = okServers;
    }

    public Set<String> getFailServers() {
        return failServers;
    }

    public void setFailServers(Set<String> failServers) {
        this.failServers = failServers;
    }
    
    public void addOkServers(String server) {
        this.okServers.add(server);
    }
    
    public void addFailServers(String server) {
        this.failServers.add(server);
    }

}
