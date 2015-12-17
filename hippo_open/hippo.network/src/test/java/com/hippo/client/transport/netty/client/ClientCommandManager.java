package com.hippo.client.transport.netty.client;

import com.hippo.network.BaseCommandManager;
import com.hippo.network.command.EchoCommand;

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
