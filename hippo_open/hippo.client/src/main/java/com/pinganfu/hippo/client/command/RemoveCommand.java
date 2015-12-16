package com.pinganfu.hippo.client.command;

import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;

/**
 * 
 * @author saitxuc
 * 2015-3-4
 */
public class RemoveCommand extends Command {

    private static final long serialVersionUID = -5219022946968142883L;

    public RemoveCommand() {
        
    }
    
    @Override
    public String getAction() {
        return CommandConstants.REMOVE_COMMAND_ACTION;
    }
    
}
