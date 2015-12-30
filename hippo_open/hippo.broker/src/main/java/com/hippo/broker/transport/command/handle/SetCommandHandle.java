package com.hippo.broker.transport.command.handle;

import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class SetCommandHandle implements CommandHandle{
	
	private CommandHandle realHandler;
	
	public SetCommandHandle(CommandHandle realHandler) {
		this.realHandler = realHandler;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		CommandResult result = realHandler.doCommand(command);
		return result;
	}
	
}
