package com.hippo.broker.transport.command.handle;

import com.hippo.broker.Broker;
import com.hippo.client.HippoResult;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class UpdateCommandHandle implements CommandHandle{
	
	private CommandHandle commandHandle;
	
	public UpdateCommandHandle(CommandHandle commandHandle) {
		this.commandHandle = commandHandle;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		CommandResult result = commandHandle.doCommand(command);
		return result;
	}
}
