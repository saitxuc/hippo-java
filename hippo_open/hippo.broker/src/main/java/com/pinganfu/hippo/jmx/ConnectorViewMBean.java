package com.pinganfu.hippo.jmx;

import com.pinganfu.hippo.common.lifecycle.LifeCycle;

/**
 * 
 * @author saitxuc 
 * 2015-3-16
 */
public interface ConnectorViewMBean extends LifeCycle {

	@MBeanInfo("Connection count")
	int connectionCount();

	/**
	 * Resets the statistics
	 */
	@MBeanInfo("Resets the statistics")
	void resetStatistics();

	/**
	 * enable statistics gathering
	 */
	@MBeanInfo("Enables statistics gathering")
	void enableStatistics();

	/**
	 * disable statistics gathering
	 */
	@MBeanInfo("Disables statistics gathering")
	void disableStatistics();

	/**
	 * Returns true if statistics is enabled
	 * 
	 * @return true if statistics is enabled
	 */
	@MBeanInfo("Statistics gathering enabled")
	boolean isStatisticsEnabled();

}
