package com.hippo.broker.transport.command.handle;

import com.hippo.broker.Broker;
import com.hippo.client.HippoResult;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class UpdateCommandHandle implements CommandHandle{
	
	private Broker broker;
	
	public UpdateCommandHandle(Broker broker) {
		this.broker = broker;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		HippoResult result = broker.processCommand(command);
		return result;
	}
}
