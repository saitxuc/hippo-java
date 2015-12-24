package com.hippo.jmx;

import java.util.List;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public interface HealthViewMBean {
	
	@MBeanInfo("List of warnings and errors about the current health of the Broker - empty list is Good!")
    List<HealthStatus> healthList() throws Exception;
	
	@MBeanInfo("String representation of current Broker state")
    String getCurrentStatus();
	
}
