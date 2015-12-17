package com.hippo.mdb.impl;

import com.hippo.mdb.CapacityController;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbManager;
import com.hippo.mdb.exception.OutOfMaxCapacityException;
import com.hippo.mdb.obj.DBInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MdbCapacityController implements CapacityController {
    private static final Logger LOG = LoggerFactory.getLogger(MdbCapacityController.class);
    private final Object mutex = new Object();
    private MdbManager mdbmanager = null;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private ExecutorService executor;
    private final BlockingQueue<DBInfo> infosPool = new LinkedBlockingQueue<DBInfo>(20);

    @Override
    public void init() {
        executor = Executors.newFixedThreadPool(1);
    }

    @Override
    public void start() {
        isStarted.compareAndSet(false, true);
        if (executor != null) {
            executor.execute(new DBInfoCreator());
            LOG.info("DBInfoCreator executor has been called to start!!");
        }
        LOG.info("MdbCapacityController finished start!!");
    }

    @Override
    public void stop() {
        LOG.info("MdbCapacityController stop has been called");
        isStarted.compareAndSet(true, false);
        if (executor != null) {
            executor.shutdownNow();
            LOG.info("DBInfoCreator executor has been called to stop!!");
        }
        LOG.info("MdbCapacityController finished calling stop");
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public Throwable getStartException() {
        return null;
    }

    public void setMdbmanager(MdbManager mdbmanager) {
        this.mdbmanager = mdbmanager;
    }

    @Override
    public DBInfo getNewDbInfo(int capacity, String b_size, String dbNo, Integer bucketNo) throws OutOfMaxCapacityException {
        DBInfo info = null;

        try {
            info = infosPool.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }

        if (info != null) {
            //LOG.info("fetch an new dbinfo " + info.getDbNo() + " from the pool!!");
            info.setBucketNo(bucketNo);
            info.setSizeModel(b_size);
        }

        return info;
    }

    @Override
    public void notifyWait() {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }

    private class DBInfoCreator implements Runnable {
        boolean isFull = false;
        private DBInfo point;

        @Override
        public void run() {
            for (; ; ) {
                if (isFull) {
                    synchronized (mutex) {
                        try {
                            mutex.wait(30000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }

                if (!isStarted.get()) {
                    break;
                }

                try {
                    if (point == null) {
                        point = mdbmanager.createDirectBufferInfo(MdbConstants.CAPACITY_SIZE, "0", null, -1);
                    }

                    boolean isSuccess = infosPool.offer(point, 500, TimeUnit.MILLISECONDS);
                    if (isSuccess) {
                        point = null;
                        isFull = false;
                    } else {
                        isFull = true;
                    }
                } catch (OutOfMaxCapacityException e) {
                    isFull = true;
                    //LOG.warn("OutOfMaxCapacityException when create the new dbinfo in the pool!!");
                } catch (InterruptedException e) {
                    isFull = true;
                }
            }
        }
    }
}
