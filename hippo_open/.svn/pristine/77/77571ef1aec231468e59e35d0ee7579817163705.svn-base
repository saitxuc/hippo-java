package com.pinganfu.hippo.broker.useage;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public class MemoryUsage extends Usage<MemoryUsage> {

    private long usage;

    public MemoryUsage() {
        this(null, null);
    }

    public MemoryUsage(MemoryUsage parent) {
        this(parent, "default");
    }

    public MemoryUsage(String name) {
        this(null, name);
    }

    public MemoryUsage(MemoryUsage parent, String name) {
        this(parent, name, 1.0f);
    }

    public MemoryUsage(MemoryUsage parent, String name, float portion) {
        super(parent, name, portion);
    }

    public void waitForSpace() throws InterruptedException {
        if (parent != null) {
            parent.waitForSpace();
        }
        synchronized (usageMutex) {
            for (int i = 0; percentUsage >= 100; i++) {
                usageMutex.wait();
            }
        }
    }

    public boolean waitForSpace(long timeout) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout)) {
                return false;
            }
        }
        synchronized (usageMutex) {
            if (percentUsage >= 100) {
                usageMutex.wait(timeout);
            }
            return percentUsage < 100;
        }
    }

    public boolean isFull() {
        if (parent != null && parent.isFull()) {
            return true;
        }
        synchronized (usageMutex) {
            return percentUsage >= 100;
        }
    }

    public void enqueueUsage(long value) throws InterruptedException {
        waitForSpace();
        increaseUsage(value);
    }

    public void increaseUsage(long value) {
        if (value == 0) {
            return;
        }
        int percentUsage;
        synchronized (usageMutex) {
            usage += value;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
        if (parent != null) {
            ((MemoryUsage)parent).increaseUsage(value);
        }
    }

    public void decreaseUsage(long value) {
        if (value == 0) {
            return;
        }
        int percentUsage;
        synchronized (usageMutex) {
            usage -= value;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
        if (parent != null) {
            parent.decreaseUsage(value);
        }
    }

    protected long retrieveUsage() {
        return usage;
    }

    public long getUsage() {
        return usage;
    }

    public void setUsage(long usage) {
        this.usage = usage;
    }

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isStarted() {
	
		return false;
	}

}
