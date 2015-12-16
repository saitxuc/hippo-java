package com.pinganfu.hippo.network.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.exception.TransportException;

/**
 * 
 * @author saitxuc
 * 2015-3-30
 */
public class TransportFactoryFinder {
	
	private static Map<String, TransportFactory> transportFactoryMap = new HashMap<String, TransportFactory>();

	static {
		ServiceLoader<TransportFactory> transportFactorys = ServiceLoader
				.load(TransportFactory.class);
		for (TransportFactory transportFactory : transportFactorys) {
			transportFactoryMap.put(transportFactory.getName(),
					transportFactory);
		}
    }
	
	public static TransportFactory getTransportFactory(String schema) {
		TransportFactory transportFactory = transportFactoryMap.get(schema);
		if (transportFactory == null) {
			throw new TransportException(
					"can't find the sender impl class of type[" + schema
							+ "]");
		}
		return transportFactory;
	}
	
	
}
