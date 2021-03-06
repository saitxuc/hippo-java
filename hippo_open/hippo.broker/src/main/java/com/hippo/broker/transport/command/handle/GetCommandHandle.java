package com.hippo.broker.transport.command.handle;

import java.io.Serializable;

import com.hippo.broker.Broker;
import com.hippo.client.HippoResult;
import com.hippo.common.Result;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 * write 2014-8-11
 */
public class GetCommandHandle implements CommandHandle{
	
	private CommandHandle commandHandler;
	
	public GetCommandHandle(CommandHandle commandHandler) {
		this.commandHandler = commandHandler;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		CommandResult result = commandHandler.doCommand(command);
		return result;
	}

}
