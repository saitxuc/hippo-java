package com.hippo.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author saitxuc
 * write 2014-7-28
 */
public class ExcutorUtils {
	
	protected static final Logger LOG = LoggerFactory.getLogger(ExcutorUtils.class);
	
	public static ScheduledExecutorService startSchedule(String scheduleName,
			Runnable scheduleTask, long init, long interval) {
		ScheduledExecutorService scheduleExecutor = Executors
				.newSingleThreadScheduledExecutor(new NamedThreadFactory("hippo-" + scheduleName, true));
		scheduleExecutor.scheduleWithFixedDelay(scheduleTask, init, interval,
				TimeUnit.MILLISECONDS);
		return scheduleExecutor;
	}

	public static ExecutorService startSingleExcutor(String name) {
		ExecutorService executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("hippo-" + name, true));
		return executorService;
	}
	
	
	public static ExecutorService startPoolExcutor(int threadPoolSize) {
		ThreadPoolExecutor executor =new ThreadPoolExecutor(1, threadPoolSize, 2L,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(threadPoolSize), new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;
	}
	
    public static void shutdown(ExecutorService executorService) {
        doShutdown(executorService, 3000);
    }
    
    private static void doShutdown(ExecutorService executorService, long shutdownAwaitTermination) {
        // code from Apache Camel - org.apache.camel.impl.DefaultExecutorServiceManager

        if (executorService == null) {
            return;
        }

        if (!executorService.isShutdown()) {
            boolean warned = false;
            StopWatch watch = new StopWatch();

            LOG.trace("Shutdown of ExecutorService: {} with await termination: {} millis", executorService, shutdownAwaitTermination);
            executorService.shutdown();

            if (shutdownAwaitTermination > 0) {
                try {
                    if (!awaitTermination(executorService, shutdownAwaitTermination)) {
                        warned = true;
                        LOG.warn("Forcing shutdown of ExecutorService: {} due first await termination elapsed.", executorService);
                        executorService.shutdownNow();
                        // we are now shutting down aggressively, so wait to see if we can completely shutdown or not
                        if (!awaitTermination(executorService, shutdownAwaitTermination)) {
                            LOG.warn("Cannot completely force shutdown of ExecutorService: {} due second await termination elapsed.", executorService);
                        }
                    }
                } catch (InterruptedException e) {
                    warned = true;
                    LOG.warn("Forcing shutdown of ExecutorService: {} due interrupted.", executorService);
                    // we were interrupted during shutdown, so force shutdown
                    executorService.shutdownNow();
                }
            }

            // if we logged at WARN level, then report at INFO level when we are complete so the end user can see this in the log
            if (warned) {
                LOG.info("Shutdown of ExecutorService: {} is shutdown: {} and terminated: {} took: {}.",
                        new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), TimeUtils.printDuration(watch.taken())});
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Shutdown of ExecutorService: {} is shutdown: {} and terminated: {} took: {}.",
                        new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), TimeUtils.printDuration(watch.taken())});
            }
        }
    }
	
    public static boolean awaitTermination(ExecutorService executorService, long shutdownAwaitTermination) throws InterruptedException {
        // log progress every 5th second so end user is aware of we are shutting down
        StopWatch watch = new StopWatch();
        long interval = Math.min(2000, shutdownAwaitTermination);
        boolean done = false;
        while (!done && interval > 0) {
            if (executorService.awaitTermination(interval, TimeUnit.MILLISECONDS)) {
                done = true;
            } else {
                LOG.info("Waited {} for ExecutorService: {} to terminate...", TimeUtils.printDuration(watch.taken()), executorService);
                // recalculate interval
                interval = Math.min(2000, shutdownAwaitTermination - watch.taken());
            }
        }

        return done;
    }
    
}
