package com.pinganfu.hippo.client.transport.netty.client;

import java.io.Serializable;

import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;
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
