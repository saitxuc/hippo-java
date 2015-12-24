package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * 
 * @author saitxuc
 *
 */
public class ExistsCommand extends Command{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3495450375329069619L;
	
	public ExistsCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.EXISTS_COMMAND_ACTION;
    }
	
}
