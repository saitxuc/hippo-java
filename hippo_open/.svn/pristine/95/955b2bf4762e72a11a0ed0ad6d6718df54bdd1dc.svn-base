package com.pinganfu.hippo.client.transport.netty.server;

import com.pinganfu.hippo.network.BaseCommandManager;
import com.pinganfu.hippo.network.command.EchoCommand;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class EchoCommandManager extends BaseCommandManager {
	
	public EchoCommandManager() {
		super();
	}
	
	@Override
	public void initConmandHandler() {
		addCommandHandler(EchoCommand.ECHO_ACTION, new EchoCommandHandler());
	}

}
