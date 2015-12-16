package com.pinganfu.hippo.broker.security;

import com.pinganfu.hippo.broker.Broker;
import com.pinganfu.hippo.broker.BrokerFilter;
import com.pinganfu.hippo.broker.transport.HippoTransportConnectionManager;
import com.pinganfu.hippo.broker.transport.TransportConnector;
import com.pinganfu.hippo.network.transport.TransportConnectionManager;

/**
 * 
 * @author saitxuc
 *
 */
public class SimpleAuthenticationBroker extends BrokerFilter implements AuthenticationManager{

	public SimpleAuthenticationBroker(Broker next) {
		super(next);
	}

	@Override
	public void login(String usename, String password) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void verfiyAuthority(String usename) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public TransportConnectionManager createTransportConnectionManager(TransportConnector connector) {
		TransportConnectionManager tcmanager = new HippoSecurityTransportConnectionManager(connector, this);
		return tcmanager;
	}
	
	
}
