package com.pinganfu.hippo.client.transport.nio;

import java.io.IOException;

import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportServer;
import com.pinganfu.hippo.network.transport.nio.NettyTransportFactory;
import com.pinganfu.hippo.network.transport.nio.client.NioTransport;
import com.pinganfu.hippo.network.transport.nio.coder.DefaultCoderInitializer;
import com.pinganfu.hippo.network.transport.nio.server.DisruptorNioTransportServer;
import com.pinganfu.hippo.network.transport.nio.server.NioTransportServer;
import com.pinganfu.hippo.network.transport.nio.server.ThreadPoolNioTransportServer;


/**
 *
 * @author saitxuc
 *
 */
public class HippoNioTransportFactory extends NettyTransportFactory {

	@Override
	public Transport connect(String host, int port, CommandManager commandManager) throws Exception {
		Transport tpctransport = new NioTransport(host, port, commandManager);

		Transport transport = compositeConfigure(tpctransport);
		transport.setCoderInitializer(new DefaultCoderInitializer());
		return transport;
	}


	@Override
	public TransportServer bind(int port, CommandManager commandManager) throws IOException {
		TransportServer server = new ThreadPoolNioTransportServer(port, commandManager);
		server.setCoderInitializer(new DefaultCoderInitializer());
		return server;
	}

	@Override
	public String getName() {
		return "netty-hippo";
	}
}
