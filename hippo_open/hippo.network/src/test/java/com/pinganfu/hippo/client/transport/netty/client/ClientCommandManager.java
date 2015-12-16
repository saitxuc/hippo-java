package com.pinganfu.hippo.client.transport.netty.client;

import com.pinganfu.hippo.network.BaseCommandManager;
import com.pinganfu.hippo.network.command.EchoCommand;

/**
 * 
 * @author saitxuc
 *
 */
public class ClientCommandManager extends BaseCommandManager {
	
	public ClientCommandManager() {
		super();
	}
	
	@Override
	public void initConmandHandler() {
		addCommandHandler(EchoCommand.ECHO_ACTION, new ClientCommandHandler());
	}
}
