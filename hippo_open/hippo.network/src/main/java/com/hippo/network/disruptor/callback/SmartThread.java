package com.hippo.network.disruptor.callback;

/**
 * 
 * @author saitxuc
 *
 */
public interface SmartThread {
	
	public void start();

    public void join() throws InterruptedException;;

    public void interrupt();

    public Boolean isSleeping();

    public void cleanup();
	
}
