package com.hippo.network.transport.nio;

import java.io.Serializable;

import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 * write 2014-7-1
 */
public interface CommandHandle {
	
	public CommandResult doCommand(Command command) throws Exception;
	
}
