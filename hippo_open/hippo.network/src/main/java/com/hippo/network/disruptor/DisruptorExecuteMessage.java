package com.hippo.network.disruptor;

import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 *
 */
public class DisruptorExecuteMessage {
	
	private Object ctx;
	
	private Command request;
	
	public DisruptorExecuteMessage(Object ctx, Command request) {
		this.ctx = ctx;
		this.request = request;
	}

	public Object getCtx() {
		return ctx;
	}

	public void setCtx(Object ctx) {
		this.ctx = ctx;
	}

	public Command getRequest() {
		return request;
	}

	public void setRequest(Command request) {
		this.request = request;
	}
	
}
