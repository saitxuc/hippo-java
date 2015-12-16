package com.pinganfu.hippo.jmx;

import com.pinganfu.hippo.common.lifecycle.LifeCycle;

/**
 * 
 * @author saitxuc 2015-3-16
 */
public interface ConnectionViewMBean extends LifeCycle {

	/**
	 * @return true if the Connection is slow
	 */
	@MBeanInfo("Connection is slow.")
	boolean isSlow();

	/**
	 * @return if after being marked, the Connection is still writing
	 */
	@MBeanInfo("Connection is blocked.")
	boolean isBlocked();

	/**
	 * @return true if the Connection is connected
	 */
	@MBeanInfo("Connection is connected to the broker.")
	boolean isConnected();

	/**
	 * @return true if the Connection is active
	 */
	@MBeanInfo("Connection is active (both connected and receiving messages).")
	boolean isActive();

	/**
	 * Resets the statistics
	 */
	@MBeanInfo("Resets the statistics")
	void resetStatistics();

	/**
	 * Returns the source address for this connection
	 * 
	 * @return the source address for this connection
	 */
	@MBeanInfo("source address for this connection")
	String getRemoteAddress();

	/**
	 * Returns the client identifier for this connection
	 * 
	 * @return the the client identifier for this connection
	 */
	@MBeanInfo("client id for this connection")
	String getClientId();

	/**
	 * Returns the User Name used to authorize creation of this Connection. This
	 * value can be null if display of user name information is disabled.
	 * 
	 * @return the name of the user that created this Connection
	 */
	@MBeanInfo("User Name used to authorize creation of this connection")
	String getUserName();
	
	@MBeanInfo("Session count")
	int sessionCount();
	

}
