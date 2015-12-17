package com.hippo.network.disruptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.hippo.network.disruptor.callback.BaseExecutor;
import com.hippo.network.disruptor.callback.RunnableCallback;

/**
 * 
 * @author saitxuc
 *
 */
public class DisruptorExecutor extends BaseExecutor implements EventHandler{
	
	protected static final Logger LOG = LoggerFactory.getLogger(DisruptorExecutor.class);
	
	private DisruptorQueue exeQueue;
	
	private DisruptorHandle eventHandle;
	
	public DisruptorExecutor(DisruptorQueue exeQueue, DisruptorHandle eventHandle) {
		this.exeQueue = exeQueue;
		this.eventHandle = eventHandle;
	}
	
	@Override
    public void run() {
		try {
            exeQueue.consumeBatchWhenAvailable(this);

        } catch (Throwable e) {
               LOG.error(idStr + " bolt exeutor  error", e);
        }
	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
		if (event == null) {
            return;
        }

        long start = System.nanoTime();
        try {
        	if (event instanceof DisruptorExecuteMessage) {
        		this.eventHandle.handEvent((DisruptorExecuteMessage)event);
        	}
        }finally {
            long end = System.nanoTime();
            //LOG.info(" hand event cost is  " + ((end - start) / 1000000.0d));
        }
	}
	
}
