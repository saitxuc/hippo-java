/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hippo.leveldb.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hippo.leveldb.Options;

public class Finalizer<T>
{
    public static final FinalizerMonitor IGNORE_FINALIZER_MONITOR = new FinalizerMonitor()
    {
        public void unexpectedException(Throwable throwable)
        {
        }
    };

    private final int threads;
    private final FinalizerMonitor monitor;

    private final ConcurrentHashMap<FinalizerPhantomReference<T>, Object> references = new ConcurrentHashMap<FinalizerPhantomReference<T>, Object>();
    private final ReferenceQueue<T> referenceQueue = new ReferenceQueue<T>();
    private boolean destroyed;
    private ExecutorService executor;
    
    public Finalizer()
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads)
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads, FinalizerMonitor monitor)
    {
        this.monitor = monitor;
        Preconditions.checkArgument(threads >= 1, "threads must be at least 1");
        this.threads = threads;
    }

    private final Bell bell = new Bell(30000);
    private static final long GARBAGE = 8 * 1024 * 1024 * 1024l; // 10g,当文件已经删除，且没有释放的空间达到该值时，则通过full gc来释放
    
    public synchronized void addCleanup(T item, Callable<?> cleanup)
    {
        Preconditions.checkNotNull(item, "item is null");
        Preconditions.checkNotNull(cleanup, "cleanup is null");
        Preconditions.checkState(!destroyed, "%s is destroyed", getClass().getName());
        
        /*
         * 暂时不考虑多线程，目前只有只有一个实例.
         */
        if (bell.ring()) {
        	StringBuilder info = new StringBuilder("");
        	long size = references.size();
        	info.append("\n" + size + " scrap files have not been garbage collected");
        	
        	if (size * Options.TARGET_FILE_SIZE > GARBAGE) {
        		System.gc();
        		info.append(", Fire full-gc if -XX:+DisableExplicitGC not configured");
        	}
        	info.append(". \n");
        	System.out.println(info.toString());
        }
        
        if (executor == null) {
            // create executor
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("FinalizerQueueProcessor-%d")
                    .setDaemon(true)
                    .build();
            executor = Executors.newFixedThreadPool(threads, threadFactory);

            // start queue processor jobs
            for (int i = 0; i < threads; i++) {
                executor.submit(new FinalizerQueueProcessor());
            }
        }

        // create a reference to the item so we are notified when it is garbage collected
        FinalizerPhantomReference<T> reference = new FinalizerPhantomReference<T>(item, referenceQueue, cleanup);

        // we must keep a strong reference to the reference object so we are notified when the item
        // is no longer reachable (if the reference object is garbage collected we are never notified)
        references.put(reference, Boolean.TRUE);
    }
    
    public synchronized void destroy()
    {
        destroyed = true;
        if( executor!=null ) {
            executor.shutdownNow();
        }
        for(FinalizerPhantomReference<T> r: references.keySet() ) {
            try {
                r.cleanup();
            } catch (Exception e) {
            }
        }
    }

    public interface FinalizerMonitor
    {
        void unexpectedException(Throwable throwable);
    }

    private static class FinalizerPhantomReference<T> extends PhantomReference<T>
    {
        private final AtomicBoolean cleaned = new AtomicBoolean(false);
        private final Callable<?> cleanup;

        private FinalizerPhantomReference(T referent, ReferenceQueue<? super T> queue, Callable<?> cleanup)
        {
            super(referent, queue);
            this.cleanup = cleanup;
        }

        private void cleanup() throws Exception {
            if(cleaned.compareAndSet(false, true)) {
                cleanup.call();
            }
        }
    }

    private class FinalizerQueueProcessor implements Runnable
    {
        @Override
        public void run()
        {
            while (!destroyed) {
                // get the next reference to cleanup
                FinalizerPhantomReference<T> reference;
                try {
                    reference = (FinalizerPhantomReference<T>) referenceQueue.remove();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // remove the reference object itself from our list of references
                references.remove(reference);

                boolean rescheduleAndReturn = false;
                try {
                    reference.cleanup();
                    rescheduleAndReturn = Thread.currentThread().isInterrupted();
                }
                catch (Throwable userException) {
                    try {
                        monitor.unexpectedException(userException);
                    }
                    catch (Exception ignored) {
                        // todo consider a broader notification
                    }

                    if (userException instanceof InterruptedException) {
                        rescheduleAndReturn = true;
                        Thread.currentThread().interrupt();
                    }
                    else if (userException instanceof Error) {
                        rescheduleAndReturn = true;
                    }
                }

                if (rescheduleAndReturn) {
                    synchronized (Finalizer.this) {
                        if (!destroyed) {
                            executor.submit(new FinalizerQueueProcessor());
                        }
                    }
                    return;
                }
            }
        }
    }
    
    public static class Bell {
    	private long preTime;
    	private final long cycle;

		public Bell(long cycle) {
			this.cycle = cycle;
			this.preTime = System.currentTimeMillis();
		}
    	
    	public synchronized boolean ring() {
			long curTime = System.currentTimeMillis();
			if (curTime - preTime >= cycle) {
				preTime = curTime;
				return true;
			}
			return false;
		}
    }
    
}
