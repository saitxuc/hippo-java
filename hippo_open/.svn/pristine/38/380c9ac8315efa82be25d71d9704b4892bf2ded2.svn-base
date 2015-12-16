package com.pinganfu.hippo.broker.security;

import com.pinganfu.hippo.broker.transport.HippoTransportConnectionManager;
import com.pinganfu.hippo.broker.transport.TransportConnector;
import com.pinganfu.hippo.network.command.ConnectionInfo;

/**
 * 
 * @author saitxuc
 *
 */
public class HippoSecurityTransportConnectionManager extends HippoTransportConnectionManager {
	
	private AuthenticationManager authManager = null;
	
	public HippoSecurityTransportConnectionManager(TransportConnector connector) {
		super(connector);
	}
	
	public HippoSecurityTransportConnectionManager(TransportConnector connector, 
			AuthenticationManager authManager) {
		super(connector);
		this.authManager = authManager;
	}
	
	@Override
	public void addConnectionInfo(Object key, ConnectionInfo info) throws Exception {
		if(this.authManager != null) {
			this.authManager.login(info.getUserName(), info.getPassword());
		}
		super.addConnectionInfo(key, info);
	}
	
}
