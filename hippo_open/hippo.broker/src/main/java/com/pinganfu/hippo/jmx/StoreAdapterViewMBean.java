package com.pinganfu.hippo.jmx;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public interface StoreAdapterViewMBean {
	
	@MBeanInfo("Name of this store engine adapter.")
    String getName();
	
	@MBeanInfo("Current data.")
    String getData();

    @MBeanInfo("Current key count.")
    long getSize();
	
    @MBeanInfo("Current memary used size.")
    long getCurrentUsedCapacity();
}
