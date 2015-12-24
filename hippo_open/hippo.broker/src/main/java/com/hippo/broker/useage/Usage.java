package com.hippo.broker.useage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.common.lifecycle.LifeCycleSupport;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 * @param <T>
 */
public abstract class Usage<T extends Usage> implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(Usage.class);
    protected final Object usageMutex = new Object();
    protected int percentUsage;
    protected T parent;
    private UsageCapacity limiter = new DefaultUsageCapacity();
    private int percentUsageMinDelta = 1;
    private final List<UsageListener> listeners = new CopyOnWriteArrayList<UsageListener>();
    private final boolean debug = LOG.isDebugEnabled();
    protected String name;
    private float usagePortion = 1.0f;
    private final List<T> children = new CopyOnWriteArrayList<T>();
    private final List<Runnable> callbacks = new LinkedList<Runnable>();
    private int pollingTime = 100;
    private final AtomicBoolean started=new AtomicBoolean();
    private ThreadPoolExecutor executor;
    private Throwable startException;
    
    public Usage(T parent, String name, float portion) {
        this.parent = parent;
        this.usagePortion = portion;
        if (parent != null) {
            this.limiter.setLimit((long)(parent.getLimit() * portion));
            name = parent.name + ":" + name;
        }
        this.name = name;
    }

    protected abstract long retrieveUsage();

    /**
     * @throws InterruptedException
     */
    public void waitForSpace() throws InterruptedException {
        waitForSpace(0);
    }

    public boolean waitForSpace(long timeout) throws InterruptedException {
        return waitForSpace(timeout, 100);
    }
    
    /**
     * @param timeout
     * @throws InterruptedException
     * @return true if space
     */
    public boolean waitForSpace(long timeout, int highWaterMark) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout, highWaterMark)) {
                return false;
            }
        }
        synchronized (usageMutex) {
            percentUsage=caclPercentUsage();
            if (percentUsage >= highWaterMark) {
                long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
                long timeleft = deadline;
                while (timeleft > 0) {
                    percentUsage=caclPercentUsage();
                    if (percentUsage >= highWaterMark) {
                        usageMutex.wait(pollingTime);
                        timeleft = deadline - System.currentTimeMillis();
                    } else {
                        break;
                    }
                }
            }
            return percentUsage < highWaterMark;
        }
    }

    public boolean isFull() {
        return isFull(100);
    }
    
    public boolean isFull(int highWaterMark) {
        if (parent != null && parent.isFull(highWaterMark)) {
            return true;
        }
        synchronized (usageMutex) {
            percentUsage=caclPercentUsage();
            return percentUsage >= highWaterMark;
        }
    }

    public void addUsageListener(UsageListener listener) {
        listeners.add(listener);
    }

    public void removeUsageListener(UsageListener listener) {
        listeners.remove(listener);
    }

    public long getLimit() {
        synchronized (usageMutex) {
            return limiter.getLimit();
        }
    }

    public void setLimit(long limit) {
        if (percentUsageMinDelta < 0) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        synchronized (usageMutex) {
            this.limiter.setLimit(limit);
            this.usagePortion = 0;
        }
        onLimitChange();
    }

    protected void onLimitChange() {
        // We may need to calculate the limit
        if (usagePortion > 0 && parent != null) {
            synchronized (usageMutex) {
                this.limiter.setLimit((long)(parent.getLimit() * usagePortion));
            }
        }
        // Reset the percent currently being used.
        int percentUsage;
        synchronized (usageMutex) {
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
        // Let the children know that the limit has changed. They may need to
        // set
        // their limits based on ours.
        for (T child : children) {
            child.onLimitChange();
        }
    }

    public float getUsagePortion() {
        synchronized (usageMutex) {
            return usagePortion;
        }
    }

    public void setUsagePortion(float usagePortion) {
        synchronized (usageMutex) {
            this.usagePortion = usagePortion;
        }
        onLimitChange();
    }

    public int getPercentUsage() {
        synchronized (usageMutex) {
            return percentUsage;
        }
    }

    public int getPercentUsageMinDelta() {
        synchronized (usageMutex) {
            return percentUsageMinDelta;
        }
    }

    public void setPercentUsageMinDelta(int percentUsageMinDelta) {
        if (percentUsageMinDelta < 1) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater than 0");
        }
        int percentUsage;
        synchronized (usageMutex) {
            this.percentUsageMinDelta = percentUsageMinDelta;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    public long getUsage() {
        synchronized (usageMutex) {
            return retrieveUsage();
        }
    }

    protected void setPercentUsage(int value) {
        synchronized (usageMutex) {
            int oldValue = percentUsage;
            percentUsage = value;
            if (oldValue != value) {
                fireEvent(oldValue, value);
            }
        }
    }

    protected int caclPercentUsage() {
        if (limiter.getLimit() == 0) {
            return 0;
        }
        return (int)((((retrieveUsage() * 100) / limiter.getLimit()) / percentUsageMinDelta) * percentUsageMinDelta);
    }

    private void fireEvent(final int oldPercentUsage, final int newPercentUsage) {
        if (debug) {
            LOG.debug(getName() + ": usage change from: " + oldPercentUsage + "% of available memory, to: " 
                + newPercentUsage + "% of available memory");
        }   
        if (started.get()) {
            // Switching from being full to not being full..
            if (oldPercentUsage >= 100 && newPercentUsage < 100) {
                synchronized (usageMutex) {
                    usageMutex.notifyAll();
                    if (!callbacks.isEmpty()) {
                        for (Iterator<Runnable> iter = new ArrayList<Runnable>(callbacks).iterator(); iter.hasNext();) {
                            Runnable callback = iter.next();
                            getExecutor().execute(callback);
                        }
                        callbacks.clear();
                    }
                }
            }
            if (!listeners.isEmpty()) {
                // Let the listeners know on a separate thread
                Runnable listenerNotifier = new Runnable() {
                    public void run() {
                        for (Iterator<UsageListener> iter = listeners.iterator(); iter.hasNext();) {
                            UsageListener l = iter.next();
                            l.onUsageChanged(Usage.this, oldPercentUsage, newPercentUsage);
                        }
                    }
                };
                if (started.get()) {
                    getExecutor().execute(listenerNotifier);
                } else {
                    LOG.warn("Not notifying memory usage change to listeners on shutdown");
                }
            }
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Usage(" + getName() + ") percentUsage=" + percentUsage
                + "%, usage=" + retrieveUsage() + ", limit=" + limiter.getLimit()
                + ", percentUsageMinDelta=" + percentUsageMinDelta + "%"
                + (parent != null ? ";Parent:" + parent.toString() : "");
    }

    @SuppressWarnings("unchecked")
    public void start() {
        if (started.compareAndSet(false, true)){
            if (parent != null) {
                parent.addChild(this);
            }
            for (T t:children) {
                t.start();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void stop() {
        if (started.compareAndSet(true, false)){
            if (parent != null) {
                parent.removeChild(this);
            }
            
            //clear down any callbacks
            synchronized (usageMutex) {
                usageMutex.notifyAll();
                for (Iterator<Runnable> iter = new ArrayList<Runnable>(this.callbacks).iterator(); iter.hasNext();) {
                    Runnable callback = iter.next();
                    callback.run();
                }
                this.callbacks.clear();
            }
            for (T t:children) {
                t.stop();
            }
        }
    }

    protected void addChild(T child) {
        children.add(child);
        if (started.get()) {
            child.start();
        }
    }

    protected void removeChild(T child) {
        children.remove(child);
    }

    /**
     * @param callback
     * @return true if the UsageManager was full. The callback will only be
     *         called if this method returns true.
     */
    public boolean notifyCallbackWhenNotFull(final Runnable callback) {
        if (parent != null) {
            Runnable r = new Runnable() {

                public void run() {
                    synchronized (usageMutex) {
                        if (percentUsage >= 100) {
                            callbacks.add(callback);
                        } else {
                            callback.run();
                        }
                    }
                }
            };
            if (parent.notifyCallbackWhenNotFull(r)) {
                return true;
            }
        }
        synchronized (usageMutex) {
            if (percentUsage >= 100) {
                callbacks.add(callback);
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * @return the limiter
     */
    public UsageCapacity getLimiter() {
        return this.limiter;
    }

    /**
     * @param limiter the limiter to set
     */
    public void setLimiter(UsageCapacity limiter) {
        this.limiter = limiter;
    }

    /**
     * @return the pollingTime
     */
    public int getPollingTime() {
        return this.pollingTime;
    }

    /**
     * @param pollingTime the pollingTime to set
     */
    public void setPollingTime(int pollingTime) {
        this.pollingTime = pollingTime;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getParent() {
        return parent;
    }

    public void setParent(T parent) {
        this.parent = parent;
    }
    
    public void setExecutor (ThreadPoolExecutor executor) {
        this.executor = executor;
    }
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }
    
    public Throwable getStartException() {
    	return startException;
    }
}
