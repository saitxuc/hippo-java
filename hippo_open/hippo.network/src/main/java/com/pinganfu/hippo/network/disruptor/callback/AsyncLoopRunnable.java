package com.pinganfu.hippo.network.disruptor.callback;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.network.disruptor.DisruptorUtils;


/**
 * 
 * @author saitxuc
 *
 */
public class AsyncLoopRunnable implements Runnable {
	
	protected static final Logger LOG = LoggerFactory.getLogger(AsyncLoopRunnable.class);
	
	private static AtomicBoolean shutdown = new AtomicBoolean(false);

    public static AtomicBoolean getShutdown() {
        return shutdown;
    }

    private RunnableCallback fn;
    private RunnableCallback killfn;
    private long lastTime = System.currentTimeMillis();

    public AsyncLoopRunnable(RunnableCallback fn, RunnableCallback killfn) {
        this.fn = fn;
        this.killfn = killfn;
    }

    private boolean needQuit(Object rtn) {
        if (rtn != null) {
            long sleepTime = Long.parseLong(String.valueOf(rtn));
            if (sleepTime < 0) {
                return true;
            } else if (sleepTime > 0) {
                long now = System.currentTimeMillis();
                long cost = now - lastTime;
                long sleepMs = sleepTime * 1000 - cost;
                if (sleepMs > 0) {
                    DisruptorUtils.sleepMs(sleepMs);
                    lastTime = System.currentTimeMillis();
                } else {
                    lastTime = now;
                }

            }
        }
        return false;
    }

    private void shutdown() {
        fn.postRun();
        fn.shutdown();
        LOG.info("Succefully shutdown");
    }

    @Override
    public void run() {

        if (fn == null) {
            LOG.error("fn==null");
            throw new RuntimeException("AsyncLoopRunnable no core function ");
        }

        fn.preRun();

        try {
            while (shutdown.get() == false) {
                Exception e = null;

                fn.run();

                if (shutdown.get() == true) {
                    shutdown();
                    return;
                }

                e = fn.error();
                if (e != null) {
                    throw e;
                }
                Object rtn = fn.getResult();
                if (this.needQuit(rtn)) {
                    shutdown();
                    return;
                }

            }
        } catch (Throwable e) {
            if (shutdown.get() == true) {
                shutdown();
                return;
            } else {
                LOG.error("Async loop died!!!" + e.getMessage(), e);
                //killfn.execute(e);
            }

        }

    }
}
