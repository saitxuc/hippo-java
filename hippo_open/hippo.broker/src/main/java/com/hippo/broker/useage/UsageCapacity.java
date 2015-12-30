package com.hippo.broker.useage;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public interface UsageCapacity{

    /**
     * Has the limit been reached ?
     * 
     * @param size
     * @return true if it has
     */
    boolean isLimit(long size);
    
    
    /**
     * @return the limit
     */
    long getLimit();
    
    /**
     * @param limit the limit to set
     */
    void setLimit(long limit);
}
