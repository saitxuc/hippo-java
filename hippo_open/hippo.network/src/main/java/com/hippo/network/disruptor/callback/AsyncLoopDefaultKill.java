package com.hippo.network.disruptor.callback;

import com.hippo.network.disruptor.DisruptorUtils;


/**
 * 
 * @author saitxuc
 *
 */
public class AsyncLoopDefaultKill extends RunnableCallback {
	
    @Override
    public void run() {
    	DisruptorUtils.halt_process(1, "Async loop died!");
    }
    
	
}
