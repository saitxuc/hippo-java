package com.pinganfu.hippo.broker.cluster.server;

import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.transport.nio.server.NioTransportServer;

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
