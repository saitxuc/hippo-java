package com.hippoconsoleweb.service;

import java.io.IOException;

import com.hippo.common.lifecycle.LifeCycleSupport;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public abstract class AbstractLocker extends LifeCycleSupport implements Locker {
	
	public static final long DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL = 10 * 1000;

    protected String name;
    protected boolean failIfLocked = false;
    protected long lockAcquireSleepInterval = DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL;

    @Override
    public boolean keepAlive() throws IOException {
        return true;
    }

    @Override
    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) {
        this.lockAcquireSleepInterval = lockAcquireSleepInterval;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setFailIfLocked(boolean failIfLocked) {
        this.failIfLocked = failIfLocked;
    }
	
}
