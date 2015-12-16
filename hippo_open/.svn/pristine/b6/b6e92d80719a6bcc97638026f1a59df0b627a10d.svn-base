package com.pinganfu.hippo.common.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * 
 * @author saitxuc
 * 2015-4-15
 */
public class HippoClusterTableInfo implements java.io.Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4615536379585116313L;
	
	private int version;
	
	private Map<Integer, Vector<String>> tableMap = new HashMap<Integer, Vector<String>>();
	
	public HippoClusterTableInfo() {
		
	}
	
	public void incVersion() {
	    version++;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Map<Integer, Vector<String>> getTableMap() {
		return tableMap;
	}

	public void setTableMap(Map<Integer, Vector<String>> tableMap) {
		this.tableMap = tableMap;
	}
	
}
