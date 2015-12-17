package com.hippo.broker.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.hippo.broker.transport.HippoTransportConnectionManager;
import com.hippo.common.exception.HippoException;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandManager;
import com.hippo.network.ServerFortress;
import com.hippo.network.ServerFortressFactory;
import com.hippo.network.impl.TransportServerFortressFactory;

/**
 * 
 * @author saitxuc
 *
 */
public class TransportConnectorFactory {
	
	protected static final Logger LOG = LoggerFactory.getLogger(TransportConnectorFactory.class);
	
	public static TransportConnector create(String nioType, String shema, int bport, CommandManager commandManager, Serializer serializer) throws HippoException {
    	TransportConnector connector = null;
		try {
			ServerFortressFactory sfactory = new TransportServerFortressFactory(shema, bport, commandManager, null);
			if(!StringUtils.isEmpty(nioType)) {
				sfactory.setNioType(nioType);
			}
			ServerFortress serverFortress = sfactory.createServer();
			connector = new TransportConnector(serverFortress);
			serverFortress.setTransportConnectionManager(new HippoTransportConnectionManager(connector));
		} catch (Exception e) {
			LOG.error(" create Transport Connector happen error.", e);
		}
		return connector;
    	
    }
    
}
