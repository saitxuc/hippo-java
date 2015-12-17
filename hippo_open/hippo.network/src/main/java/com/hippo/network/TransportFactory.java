package com.hippo.network;

import java.io.IOException;
import java.net.URI;

import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportServer;

/**
 * 
 * @author saitxuc
 * write 2014-7-15
 */
public interface TransportFactory {
    
    Transport connect(URI uri, CommandManager commandManager) throws Exception;
	
	Transport connect(String host, int port, CommandManager commandManager) throws Exception;
	
    public TransportServer bind(int port, CommandManager commandManager) throws IOException;
	
	String getName();
	
}
