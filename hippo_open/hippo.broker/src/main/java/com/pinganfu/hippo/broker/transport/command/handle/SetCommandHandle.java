package com.pinganfu.hippo.broker.transport.command.handle;

import java.io.Serializable;

import com.pinganfu.hippo.broker.Broker;
import com.pinganfu.hippo.client.HippoResult;
import com.pinganfu.hippo.common.Result;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 *
 */
public class SetCommandHandle implements CommandHandle{
	
	private Broker broker;
	
	public SetCommandHandle(Broker broker) {
		this.broker = broker;
	}
	
	@Override
	public CommandResult doCommand(Command command) throws Exception {
		HippoResult result = broker.processCommand(command);
		return result;
	}
	
}
