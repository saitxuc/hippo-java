package com.hippo.broker.transport.command.handle;

import com.hippo.broker.Broker;
import com.hippo.client.HippoResult;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * Created by Owen on 2015/11/26.
 */
public class BitGetCommandHandle implements CommandHandle {

    private CommandHandle commandHandler;

    public BitGetCommandHandle(CommandHandle commandHandler) {
        this.commandHandler = commandHandler;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
    	CommandResult result = commandHandler.doCommand(command);
        return result;
    }
}