package com.hippo.broker.transport.command.handle;

import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class HippoCommandHandle implements CommandHandle {

    private CommandHandle commandHandler;

    public HippoCommandHandle(CommandHandle commandHandler) {
        this.commandHandler = commandHandler;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
    	CommandResult result = commandHandler.doCommand(command);
        return result;
    }
}
