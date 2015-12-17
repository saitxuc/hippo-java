package com.hippo.network.transport.nio;

import java.io.IOException;
import java.net.URI;

import com.hippo.common.exception.HippoException;
import com.hippo.network.CommandManager;
import com.hippo.network.TransportFactory;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.client.RecoveryNioTransport;
import com.hippo.network.transport.nio.client.ResponseCorrelator;
import com.hippo.network.transport.nio.server.NioTransportServer;

/**
 * 
 * @author saitxuc
 * 2014-1-15
 */
public class RecoveryTransportFactory implements TransportFactory{

	@Override
	public Transport connect(String host, int port, CommandManager commandManager)
			throws Exception {
		Transport tpctransport = new RecoveryNioTransport(host, port, commandManager); 
		Transport transport = compositeConfigure(tpctransport);
		return transport;
	}

	@Override
	public TransportServer bind(int port, CommandManager commandManager) throws IOException {
		TransportServer server = new NioTransportServer(port, commandManager);
		return server;
	}

	@Override
	public String getName() {
		return "netty-recovery";
	}

	private Transport compositeConfigure(Transport transport) {
		return new ResponseCorrelator(transport);
	}

    @Override
    public Transport connect(URI uri, CommandManager commandManager) throws Exception {
        if(uri == null) {
            throw new HippoException("uri is null");
        }
        return connect(uri.getHost(), uri.getPort(), commandManager);
    }
	
}
