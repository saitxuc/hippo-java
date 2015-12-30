package com.hippo.broker.transport;

import com.hippo.broker.transport.command.handle.HippoCommandHandle;
import com.hippo.network.BaseCommandManager;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class HippoBrokerCommandManager extends BaseCommandManager<String>{
	
	private CommandHandle commandHandler;
	
	public HippoBrokerCommandManager() {
		super();
	}
	
	public HippoBrokerCommandManager(CommandHandle realHandler) {
		this.commandHandler = realHandler;
		this.init();
	}
	
	@Override
	public void initConmandHandler() {
		addCommandHandler(CommandConstants.SET_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
		addCommandHandler(CommandConstants.UPDATE_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
		addCommandHandler(CommandConstants.GET_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
		addCommandHandler(CommandConstants.REMOVE_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
        addCommandHandler(CommandConstants.ATOMICNT_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
        addCommandHandler(CommandConstants.BITGET_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
        addCommandHandler(CommandConstants.BITSET_COMMAND_ACTION, new HippoCommandHandle(commandHandler));
	}

	public void setCommandHandle(CommandHandle commandHandler) {
		this.commandHandler = commandHandler;
	}
	
}
