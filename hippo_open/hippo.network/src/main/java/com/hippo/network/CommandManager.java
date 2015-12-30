package com.hippo.network;

import com.hippo.network.command.Command;
import java.io.Serializable;

/**
 * 
 * @author saitxuc
 * write 2014-7-4
 */
public interface CommandManager<T> {
	
	public CommandResult handleCommand(Command connmand, T commandKey);
	
}
