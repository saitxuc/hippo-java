package com.hippo.redis.command.handler;

import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class RedisOpCommandHandler implements CommandHandle {
	
	private CommandHandle realHandler;
	
	public RedisOpCommandHandler(CommandHandle realHandler) {
		this.realHandler = realHandler;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		return realHandler.doCommand(command);
	}

}
