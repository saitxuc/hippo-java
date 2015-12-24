package com.hippo.broker.security;

import com.hippo.broker.Broker;
import com.hippo.broker.plugin.BrokerPlugin;

/**
 * 
 * @author saitxuc
 * 2015-4-27
 */
public class AuthorizationPlugin implements BrokerPlugin {
	
	public AuthorizationPlugin() {
		
	}
	
	@Override
	public Broker installPlugin(Broker broker) throws Exception {
		return new SimpleAuthenticationBroker(broker);
	}
	
	
	
}
