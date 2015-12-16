package com.pinganfu.hippo.broker;

import java.net.URI;

/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public interface BrokerFactoryHandler {
	
	BrokerService createBroker(URI brokerURI) throws Exception;
	
}
