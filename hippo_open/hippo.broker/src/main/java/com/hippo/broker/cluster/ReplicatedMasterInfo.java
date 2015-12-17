package com.hippo.broker.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author saitxuc
 * 2015-1-15
 */
public class ReplicatedMasterInfo implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8831432574002867444L;
	private List<String> buckets = new ArrayList<String>();
	
	public ReplicatedMasterInfo() {
		super();
	}

	public List<String> getBuckets() {
		return buckets;
	}

	public void setBuckets(List<String> buckets) {
		this.buckets = buckets;
	}
	
}
