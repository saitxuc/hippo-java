package com.pinganfu.hippoconsoleweb.service;

import java.io.IOException;

import com.pinganfu.hippo.common.lifecycle.LifeCycle;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public interface Locker extends LifeCycle {
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
    boolean keepAlive() throws IOException;

    /**
     * 
     * @param lockAcquireSleepInterval
     */
    void setLockAcquireSleepInterval(long lockAcquireSleepInterval);

    /**
     * 
     * @param name
     */
    public void setName(String name);

    /**
     * 
     * @param failIfLocked
     */
    public void setFailIfLocked(boolean failIfLocked);
	
}
