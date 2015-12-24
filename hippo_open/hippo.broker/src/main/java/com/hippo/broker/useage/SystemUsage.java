package com.hippo.broker.useage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.store.StoreEngine;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public class SystemUsage extends LifeCycleSupport {

    private SystemUsage parent;
    private String name;
    private MemoryUsage memoryUsage;
    private StoreUsage storeUsage;
    private ThreadPoolExecutor executor;
    
    private boolean sendFailIfNoSpaceExplicitySet;
    private boolean sendFailIfNoSpace;
    private boolean sendFailIfNoSpaceAfterTimeoutExplicitySet;
    private long sendFailIfNoSpaceAfterTimeout = 0;

    private final List<SystemUsage> children = new CopyOnWriteArrayList<SystemUsage>();

    public SystemUsage() {
        this("default", null);
    }

    public SystemUsage(String name, StoreEngine engine) {
        this.parent = null;
        this.name = name;
        this.memoryUsage = new MemoryUsage(name + ":memory");
        this.storeUsage = new StoreUsage(name + ":store", engine);
       this.memoryUsage.setExecutor(getExecutor());
        this.storeUsage.setExecutor(getExecutor());
    }

    public SystemUsage(SystemUsage parent, String name) {
        this.parent = parent;
        this.executor = parent.getExecutor();
        this.name = name;
        this.memoryUsage = new MemoryUsage(parent.memoryUsage, name + ":memory");
        this.storeUsage = new StoreUsage(parent.storeUsage, name + ":store");
        this.memoryUsage.setExecutor(getExecutor());
        this.storeUsage.setExecutor(getExecutor());
    }

    public String getName() {
        return name;
    }

    /**
     * @return the memoryUsage
     */
    public MemoryUsage getMemoryUsage() {
        return this.memoryUsage;
    }

    /**
     * @return the storeUsage
     */
    public StoreUsage getStoreUsage() {
        return this.storeUsage;
    }


    @Override
    public String toString() {
        return "UsageManager(" + getName() + ")";
    }


    /**
     * 
     * @param failProducerIfNoSpace
     */
    public void setSendFailIfNoSpace(boolean failProducerIfNoSpace) {
        sendFailIfNoSpaceExplicitySet = true;
        this.sendFailIfNoSpace = failProducerIfNoSpace;
    }

    public boolean isSendFailIfNoSpace() {
        if (sendFailIfNoSpaceExplicitySet || parent == null) {
            return sendFailIfNoSpace;
        } else {
            return parent.isSendFailIfNoSpace();
        }
    }

    private void addChild(SystemUsage child) {
        children.add(child);
    }

    private void removeChild(SystemUsage child) {
        children.remove(child);
    }

    public SystemUsage getParent() {
        return parent;
    }

    public void setParent(SystemUsage parent) {
        this.parent = parent;
    }

    public boolean isSendFailIfNoSpaceExplicitySet() {
        return sendFailIfNoSpaceExplicitySet;
    }

    public void setSendFailIfNoSpaceExplicitySet(boolean sendFailIfNoSpaceExplicitySet) {
        this.sendFailIfNoSpaceExplicitySet = sendFailIfNoSpaceExplicitySet;
    }

    public long getSendFailIfNoSpaceAfterTimeout() {
        if (sendFailIfNoSpaceAfterTimeoutExplicitySet || parent == null) {
            return sendFailIfNoSpaceAfterTimeout;
        } else {
            return parent.getSendFailIfNoSpaceAfterTimeout();
        }
    }

    public void setSendFailIfNoSpaceAfterTimeout(long sendFailIfNoSpaceAfterTimeout) {
        this.sendFailIfNoSpaceAfterTimeoutExplicitySet = true;
        this.sendFailIfNoSpaceAfterTimeout = sendFailIfNoSpaceAfterTimeout;
    }

    public void setName(String name) {
        this.name = name;
        this.memoryUsage.setName(name + ":memory");
        this.storeUsage.setName(name + ":store");
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
        if (memoryUsage.getName() == null) {
            memoryUsage.setName(this.memoryUsage.getName());
        }
        if (parent != null) {
            memoryUsage.setParent(parent.memoryUsage);
        }
        this.memoryUsage = memoryUsage;
        this.memoryUsage.setExecutor(getExecutor());
    }

    public void setStoreUsage(StoreUsage storeUsage) {
        if (storeUsage.getStore() == null) {
            storeUsage.setStore(this.storeUsage.getStore());
        }
        if (storeUsage.getName() == null) {
            storeUsage.setName(this.storeUsage.getName());
        }
        if (parent != null) {
            storeUsage.setParent(parent.storeUsage);
        }
        this.storeUsage = storeUsage;
        this.storeUsage.setExecutor(executor);
    }


    /**
     * @return the executor
     */
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

    /**
     * @param executor
     *            the executor to set
     */
    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
        if (this.memoryUsage != null) {
            this.memoryUsage.setExecutor(this.executor);
        }
        if (this.storeUsage != null) {
            this.storeUsage.setExecutor(this.executor);
        }
    }
    
	@Override
	public void doInit() {
		
	}

	@Override
	public void doStart() {
		if (parent != null) {
            parent.addChild(this);
        }
        this.memoryUsage.start();
        this.storeUsage.start();
	}

	@Override
	public void doStop() {
        if (parent != null) {
            parent.removeChild(this);
        }
        this.memoryUsage.stop();
        this.storeUsage.stop();
	}
}
