package com.hippo.network.disruptor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author saitxuc
 *
 */
public class Time {
	
	private static AtomicBoolean simulating = new AtomicBoolean(false);
	
	private static final Object sleepTimesLock = new Object();
	
	private static AtomicLong simulatedCurrTimeMs;
	
	private static volatile Map<Thread, AtomicLong> threadSleepTimes;
	
    public static boolean isThreadWaiting(Thread t) {
        if(!simulating.get()) throw new IllegalStateException("Must be in simulation mode");
        AtomicLong time;
        synchronized(sleepTimesLock) {
            time = threadSleepTimes.get(t);
        }
        return !t.isAlive() || time!=null && currentTimeMillis() < time.longValue();
    }  
	
    public static long currentTimeMillis() {
        if(simulating.get()) {
            return simulatedCurrTimeMs.get();
        } else {
            return System.currentTimeMillis();
        }
    }
    
}
