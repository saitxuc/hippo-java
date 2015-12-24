package com.hippo.broker;

import java.io.IOException;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public interface Lockable {
	
	/**
	 * 
	 * @param useLock
	 */
    public void setUseLock(boolean useLock);

    /**
     * 
     * @param locker
     * @throws IOException
     */
    public void setLocker(Locker locker) throws IOException;

    /**
     * 
     * @param lockKeepAlivePeriod
     */
    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod);
	
}
