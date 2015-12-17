package com.hippo.network.disruptor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * copy from jstrom
 * @author saitxuc
 *
 */
public abstract class DisruptorQueue {
	
	public static void setUseSleep(boolean useSleep) {
        DisruptorQueueImpl.setUseSleep(useSleep);
    }
    
    private static boolean CAPACITY_LIMITED = false;
    
    public static void setLimited(boolean limited) {
        CAPACITY_LIMITED = limited;
    }
    
    public static DisruptorQueue mkInstance(String queueName, ProducerType producerType, int bufferSize, WaitStrategy wait) {
        if (CAPACITY_LIMITED == true) {
            return new DisruptorQueueImpl(queueName, producerType, bufferSize, wait);
        } else {
            return new DisruptorWrapBlockingQueue(queueName, producerType, bufferSize, wait);
        }
    }
    
    public abstract String getName();
    
    public abstract void haltWithInterrupt();
    
    public abstract Object poll();
    
    public abstract Object take();
    
    public abstract void consumeBatch(EventHandler<Object> handler);
    
    public abstract void consumeBatchWhenAvailable(EventHandler<Object> handler);
    
    public abstract void publish(Object obj);
    
    public abstract void publish(Object obj, boolean block) throws InsufficientCapacityException;
    
    public abstract void consumerStarted();
    
    public abstract void clear();
    
    public abstract long population();
    
    public abstract long capacity();
    
    public abstract long writePos();
    
    public abstract long readPos();
    
    public abstract float pctFull();
	
}
