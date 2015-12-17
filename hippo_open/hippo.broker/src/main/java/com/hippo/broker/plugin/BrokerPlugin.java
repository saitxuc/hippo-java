package com.hippo.broker.plugin;

import com.hippo.broker.Broker;

/**
 * 
 * @author saitxuc
 * 2015-4-27
 */
public interface BrokerPlugin {
	
	/**
	 * 
	 * @param broker
	 * @return
	 * @throws Exception
	 */
	Broker installPlugin(Broker broker) throws Exception;
	
}
