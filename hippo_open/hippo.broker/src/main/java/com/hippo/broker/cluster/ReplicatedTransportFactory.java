package com.hippo.broker.cluster;

import java.io.IOException;

import com.hippo.broker.cluster.server.ReplicatedNioTransportServer;
import com.hippo.common.Extension;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandManager;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.NettyTransportFactory;

/**
 * 
 * @author saitxuc
 * 2015-1-15
 */
@Extension("netty-replicated")
public class ReplicatedTransportFactory extends NettyTransportFactory {
	
    @Override
    public TransportServer bind(int port, CommandManager commandManager) throws IOException {
        TransportServer server = new ReplicatedNioTransportServer(port, commandManager);
        return server;
    }
	
	@Override
	public String getName() {
		return "netty-replicated";
	}
    
}
