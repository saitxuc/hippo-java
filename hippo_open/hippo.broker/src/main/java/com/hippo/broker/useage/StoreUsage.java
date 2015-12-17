package com.hippo.broker.useage;

import com.hippo.store.StoreEngine;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public class StoreUsage extends Usage<StoreUsage> {

    private StoreEngine engine;

    public StoreUsage() {
        super(null, null, 1.0f);
    }

    public StoreUsage(String name, StoreEngine engine) {
        super(null, name, 1.0f);
        this.engine = engine;
    }

    public StoreUsage(StoreUsage parent, String name) {
        super(parent, name, 1.0f);
        this.engine = parent.engine;
    }

    protected long retrieveUsage() {
        if (engine == null)
            return 0;
        return engine.size();
    }

    public StoreEngine getStore() {
        return engine;
    }

    public void setStore(StoreEngine engine) {
        this.engine = engine;
        onLimitChange();
    }

    @Override
    public int getPercentUsage() {
        synchronized (usageMutex) {
            percentUsage = caclPercentUsage();
            return super.getPercentUsage();
        }
    }

    @Override
    public boolean waitForSpace(long timeout, int highWaterMark) throws InterruptedException {
        if (parent != null) {
            if (parent.waitForSpace(timeout, highWaterMark)) {
                return true;
            }
        }

        return super.waitForSpace(timeout, highWaterMark);
    }

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isStarted() {
		// TODO Auto-generated method stub
		return false;
	}
}
