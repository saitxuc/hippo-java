package com.hippo.network.disruptor;

/**
 * 
 * @author saitxuc
 *
 */
public interface DisruptorHandle {
	
	public void handEvent(DisruptorExecuteMessage disexe);
	
}
