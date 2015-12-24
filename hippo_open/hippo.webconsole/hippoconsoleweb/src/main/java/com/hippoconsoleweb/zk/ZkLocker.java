package com.hippoconsoleweb.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippoconsoleweb.zk.lock.LockListener;
import com.hippoconsoleweb.zk.lock.WriteLock;

public class ZkLocker {
    private static final Logger log = LoggerFactory.getLogger(ZkLocker.class);
    protected CountDownLatch latch = new CountDownLatch(1);
    private WriteLock leader = null;

    public void acquireLock(ZooKeeper zk, String lockPath) {
        leader = new WriteLock(zk, lockPath, null);
        leader.setLockListener(new LockCallback());

        try {
            leader.lock();

            // Wait for any previous leaders to die and one of our new
            // nodes to become the new leader
            latch.await();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public void unlock() {
        log.info("=======unlock");
        leader.unlock();
    }

    class LockCallback implements LockListener {
        public void lockAcquired() {
            log.info("=======lockAcquired: " + this.toString());
            latch.countDown();
        }

        public void lockReleased() {
            log.info("=======lockReleased: " + this.toString());
        }
    }
}
