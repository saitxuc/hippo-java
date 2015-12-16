package com.pinganfu.hippo.client.transport.netty.server;

import java.io.Serializable;

import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class EchoCommandHandler implements CommandHandle {

	@Override
	public CommandResult doCommand(Command command) throws Exception {
		
		System.out.println("----------doCommand-------->>>"+command.getContent());
		
		//Thread.sleep(15000);
		
		return new CommandResult(true,  "Hello World");
	}

}
