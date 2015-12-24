package com.hippo.broker.cluster.server;

import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandManager;
import com.hippo.network.transport.nio.server.NioTransportServer;

/**
 * 
 * @author saitxuc
 * 2014-1-15
 */
public class ReplicatedNioTransportServer extends NioTransportServer {
	
    public ReplicatedNioTransportServer(int port, CommandManager commandManager) {
        super(port,  commandManager);
        
    }
    
    @Override
    public void doInit() {
        super.doInit();
    }
	
}
