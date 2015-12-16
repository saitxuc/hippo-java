package com.pinganfu.hippo.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.ExcutorUtils;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public abstract class LockableServiceSupport extends LifeCycleSupport implements Lockable{
	
	protected static final Logger LOG = LoggerFactory.getLogger(LockableServiceSupport.class);
	
	boolean useLock = true;
    Locker locker;
    long lockKeepAlivePeriod = 0;
    private ScheduledFuture<?> keepAliveTicket;
    private ScheduledThreadPoolExecutor clockDaemon;
    protected BrokerService brokerService;

    @Override
    public void setUseLock(boolean useLock) {
        this.useLock = useLock;
    }

    @Override
    public void setLocker(Locker locker) throws IOException {
        this.locker = locker;
    }

    public Locker getLocker()  {
        return this.locker;
    }

    @Override
    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod) {
        this.lockKeepAlivePeriod = lockKeepAlivePeriod;
    }

    @Override
    public void doInit()  {
        if (useLock) {
            if (getLocker() == null) {
                LOG.warn("No locker configured");
            } else {
                getLocker().start();
                if (lockKeepAlivePeriod > 0) {
                    keepAliveTicket = getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            keepLockAlive();
                        }
                    }, lockKeepAlivePeriod, lockKeepAlivePeriod, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    @Override
    public void doStop()  {
        if (useLock) {
            if (keepAliveTicket != null) {
                keepAliveTicket.cancel(false);
                keepAliveTicket = null;
            }
            if (locker != null) {
                getLocker().stop();
            }
            ExcutorUtils.shutdown(clockDaemon);
        }
    }

    protected void keepLockAlive() {
        boolean stop = false;
        try {
            Locker locker = getLocker();
            if (locker != null) {
                if (!locker.keepAlive()) {
                    stop = true;
                }
            }
        } catch (IOException e) {
            LOG.warn("locker keepalive resulted in: " + e, e);
        }
        if (stop) {
            stopBroker();
        }
    }

    protected void stopBroker() {
        // we can no longer keep the lock so lets fail
        LOG.info(brokerService.getBrokerName() + ", no longer able to keep the exclusive lock so giving up being a master");
        try {
            brokerService.stop();
        } catch (Exception e) {
            LOG.warn("Failure occurred while stopping broker");
        }
    }

    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ActiveMQ Lock KeepAlive Timer");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return clockDaemon;
    }
	
	
}
