package com.hippo.client.command;

import java.util.ArrayList;
import java.util.List;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * 
 * @author saitxuc
 *
 */
public class RemoveListCommand extends Command {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1922096377622843082L;
	
	private List<RemoveCommand> removes = new ArrayList<RemoveCommand>();
	
	public RemoveListCommand() {
        
    }
    
    @Override
    public String getAction() {
        return CommandConstants.REMOVELIST_COMMAND_ACTION;
    }
    
    public void addRemoveCommand(RemoveCommand command) {
    	removes.add(command);
    }

	public List<RemoveCommand> getRemoves() {
		return removes;
	}
    
}
