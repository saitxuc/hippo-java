package com.hippo.client.transport.netty.server;

import com.hippo.network.BaseCommandManager;
import com.hippo.network.command.EchoCommand;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class EchoCommandManager extends BaseCommandManager<String> {
	
	public EchoCommandManager() {
		super();
	}
	
	@Override
	public void initConmandHandler() {
		addCommandHandler(EchoCommand.ECHO_ACTION, new EchoCommandHandler());
	}

}
