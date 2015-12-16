package com.pinganfu.hippo.network.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.ServerFortressFactory;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportConnectionManager;
import com.pinganfu.hippo.network.transport.TransportFactoryFinder;
import com.pinganfu.hippo.network.transport.TransportServer;

/**
 * 
 * @author saitxuc
 * 2015-3-30
 */
public class TransportServerFortressFactory implements ServerFortressFactory {

	protected static final Logger LOG = LoggerFactory.getLogger(TransportServerFortressFactory.class);
	
	public static final int DEFAULT_PORT = 61300;
	
	public static final String DEFAULT_SHEME  = "tcp";
	
public static final String DEFAULT_NIO_TYPE = "netty";
	
	private String nioType = DEFAULT_NIO_TYPE;
	
	private int bindPort;
	
	private String sheme = null;
	
	private TransportConnectionManager connectionManager;
	
	private CommandManager commandManager; 
	
	private static final Object lock = new Object();
	
	public TransportServerFortressFactory() {
		super();
	}
	
	public TransportServerFortressFactory(CommandManager commandManager, TransportConnectionManager connectionManager) {
		this(DEFAULT_SHEME, DEFAULT_PORT, commandManager, connectionManager);
	}
	
	public TransportServerFortressFactory(String sheme, int bindPort, 
			CommandManager commandManager, TransportConnectionManager connectionManager) {
		this.sheme = sheme;
		this.bindPort = bindPort;
		this.commandManager = commandManager;
		this.connectionManager = connectionManager;
	}
	
	@Override
	public void setCommandManager(CommandManager commandManager) {
		this.commandManager = commandManager;
	}

	@Override
	public ServerFortress createServer() throws HippoException {
		return createServer(this.bindPort, this.sheme, this.commandManager, this.connectionManager);
	}

	@Override
	public ServerFortress createServer(int bindPort, String sheme, CommandManager commandManager, TransportConnectionManager connectionManager) throws HippoException {
		ServerFortress serverFortress = null;
		try{
			String schema = createShema(sheme);
			TransportServer transportServer = createTransport(bindPort, schema, commandManager);
			serverFortress = createServerFortress(transportServer, connectionManager);
		}catch(Exception e) {
        	e.printStackTrace();
			try{
    			if(serverFortress != null) {
    				serverFortress.close();
    			}
    		}catch(Throwable ignore) {
    		}
        	HippoException ex = new HippoException("Could not bind port: " + bindPort + ". Reason: " + e.getMessage());
            ex.setLinkedException(e);
            throw ex;
        }
		return serverFortress;
	}

	@Override
	public void setNioType(String ntype) {
		this.nioType = ntype;
	}
	
	private TransportServer createTransport(int bindPort, String schema, CommandManager commandManager) throws Exception {
		synchronized (lock) {
			TransportFactory transportFactory = TransportFactoryFinder.getTransportFactory(schema);
			final TransportServer transportServer = transportFactory.bind(bindPort, commandManager);
			return transportServer;
		}
		
	}
	
	private ServerFortress createServerFortress(TransportServer transportServer, TransportConnectionManager connectionManager) throws Exception{
		ServerFortress serverFortress = new TransportServerFortress(transportServer, connectionManager); 
		transportServer.setServerFortress(serverFortress);
		return serverFortress;
	}


	@Override
	public void setConnectionManager(
			TransportConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
	
	private String createShema(String scheme) {
		return (nioType + "-" + scheme);
	}
}
