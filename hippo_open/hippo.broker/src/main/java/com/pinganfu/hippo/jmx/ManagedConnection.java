package com.pinganfu.hippo.jmx;

import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.manager.ConnectionStatistics;
import com.pinganfu.hippo.network.Connection;

/**
 * 
 * @author saitxuc
 * 2015-3-17
 */
public interface ManagedConnection extends LifeCycle {
	
	/**
	 * 
	 * @return
	 */
	ConnectionStatistics getStatistics();
	
	/**
	 * 
	 * @return
	 */
	String getConnectionIdString();
	
	/**
	 * 
	 * @return
	 */
	boolean isActive();
	
	/**
	 * 
	 * @return
	 */
	boolean isBlock();
	
	/**
	 * 
	 * @return
	 */
	boolean isConnected();
	
	/**
	 * 
	 * @return
	 */
	boolean isSlow();
	
	/**
	 * 
	 * @return
	 */
	String getRemoteAddress();
	
	/**
	 * 
	 * @return
	 */
	String getClientId();
	
	/**
	 * 
	 * @return
	 */
	String getUserName();
	
	/**
	 * 
	 * @return
	 */
	int sessionCount();
	
	/**
	 * 
	 */
	void addSession();
	
	/**
	 * 
	 */
	void delSession();
}
