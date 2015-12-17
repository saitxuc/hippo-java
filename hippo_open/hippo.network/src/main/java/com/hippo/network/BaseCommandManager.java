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
 */
public abstract class BaseCommandManager implements CommandManager {
	
	protected static final Logger LOG = LoggerFactory.getLogger(BaseCommandManager.class);
	
	private Map<String, CommandHandle> commandMap = new HashMap<String, CommandHandle>();
	
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

	public Map<String, CommandHandle> getCommandMap() {
		return commandMap;
	}

	public void setCommandMap(Map<String, CommandHandle> commandMap) {
		this.commandMap = commandMap;
	}
	
	public void addCommandHandler(String key, CommandHandle commandHandle) {
		this.commandMap.put(key, commandHandle);
	}
	
	public CommandHandle getCommandHandler(String key) {
		return this.commandMap.get(key);
	}
	
	public CommandResult handleCommand(Command command) {
		CommandHandle commandHandle = this.getCommandHandler(command.getAction());
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
