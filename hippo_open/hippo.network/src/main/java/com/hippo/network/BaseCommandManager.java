package com.hippo.network;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.network.command.Command;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 * write 2014-7-4
 * @param <T>
 */
public abstract class BaseCommandManager<T> implements CommandManager<T> {
	
	protected static final Logger LOG = LoggerFactory.getLogger(BaseCommandManager.class);
	
	private Map<T, CommandHandle> commandMap = new HashMap<T, CommandHandle>();
	
	private TransportServer server;
	
	public BaseCommandManager(TransportServer server) {
		this.server = server;
	}
	
	public void init() {
		initConmandHandler();
	}
	
	public BaseCommandManager() {
		init();
	}

	public Map<T, CommandHandle> getCommandMap() {
		return commandMap;
	}

	public void setCommandMap(Map<T, CommandHandle> commandMap) {
		this.commandMap = commandMap;
	}
	
	public void addCommandHandler(T key, CommandHandle commandHandle) {
		this.commandMap.put(key, commandHandle);
	}
	
	public CommandHandle getCommandHandler(T key) {
		return this.commandMap.get(key);
	}
	
	public CommandResult handleCommand(Command command, T commandKey) {
		CommandHandle commandHandle = this.getCommandHandler(commandKey);
		CommandResult cresult = null;
		if(commandHandle != null) {
			try {
				cresult = commandHandle.doCommand(command);
			} catch (Exception e) {
				LOG.error(" handleCommand happen error。 ", e);
			}
		}
		return cresult;
	}
	
	public abstract void initConmandHandler();
	
}
