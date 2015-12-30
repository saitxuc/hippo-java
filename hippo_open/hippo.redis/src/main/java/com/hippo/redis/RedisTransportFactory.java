package com.hippo.redis;

import java.io.IOException;

import com.hippo.common.Extension;
import com.hippo.network.CommandManager;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.NettyTransportFactory;

/**
 * 
 * @author saitxuc
 *
 */
@Extension("netty-redis")
public class RedisTransportFactory extends NettyTransportFactory{
	
	
	@Override
	public Transport connect(String host, int port, CommandManager commandManager) throws Exception {
		return null;
	}


	@Override
	public TransportServer bind(int port, CommandManager commandManager) throws IOException {
		TransportServer server = new RedisTransportServer(port, commandManager);
		//server.setCoderInitializer(new DefaultCoderInitializer());
		return server;
	}

	@Override
	public String getName() {
		return "netty-redis";
	}
	
}
