package com.hippo.network.impl;


import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.network.ServerFortress;
import com.hippo.network.transport.TransportConnectionManager;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.server.NioTransportConnectionManager;

/**
 * 
 * @author saitxuc
 * 2015-3-30
 */
public class TransportServerFortress extends LifeCycleSupport implements ServerFortress {
	
	private TransportServer transportServer = null;
	
	private TransportConnectionManager connectionManager = null;
	
	private String schema = null;
	
	public TransportServerFortress() {
		super();
	}
	
	public TransportServerFortress(TransportServer transportServer) {
		this(transportServer, null);
	}
	
	public TransportServerFortress(TransportServer transportServer, 
			TransportConnectionManager connectionManager) {
		this.transportServer = transportServer;
		this.connectionManager = connectionManager;
	}
	
	@Override
	public void close() throws HippoException {
		this.stop();
	}

	@Override
	public void doInit() {
		if(connectionManager == null) {
			connectionManager = new NioTransportConnectionManager();
		}
		transportServer.init();
	}

	@Override
	public void doStart() {
		transportServer.start();
	}

	@Override
	public void doStop() {
		connectionManager.destroy();
		transportServer.stop();
	}

	@Override
	public void setTransportConnectionManager(TransportConnectionManager manager) {
		this.connectionManager = manager;
	}

	@Override
	public TransportConnectionManager getTransportConnectionManager() {
		return this.connectionManager;
	}

	@Override
	public TransportServer getTransportServer() {
		return transportServer;
	}

	public String getSchema() {
		return schema;
	}

	public void setTransportServer(TransportServer transportServer) {
		this.transportServer = transportServer;
	}
	
}
