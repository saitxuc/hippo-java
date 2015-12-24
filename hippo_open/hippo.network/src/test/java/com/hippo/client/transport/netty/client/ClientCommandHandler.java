package com.hippo.client.transport.netty.client;

import java.io.Serializable;

import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.CommandHandle;
/**
 * 
 * @author saitxuc
 *
 */
public class ClientCommandHandler implements CommandHandle {

	@Override
	public CommandResult doCommand(Command command) throws Exception {
		
		//System.out.println("----------client---content----->>>"+((Response)command).getContent());
		
		return null;
	}
}
