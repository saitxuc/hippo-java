package com.pinganfu.hippo.client.util;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.pinganfu.hippo.client.exception.HippoClientException;
import com.pinganfu.hippo.client.listener.StartupListener;
import com.pinganfu.hippo.client.transport.AbstractClientConnectionControl;
import com.pinganfu.hippo.client.transport.cluster.ClusterConnectionControl;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;

public class ClientSPIManager {
//	private static final Logger log = LoggerFactory.getLogger(ClientSPIManager.class);
	
	/**
	 * init connectionControl for each defaultHippoClient
	 * @param schema
	 * @return
	 * @throws HippoClientException 
	 */
	public static AbstractClientConnectionControl findConnectionControl(String schema) throws HippoClientException {
		Map<String, AbstractClientConnectionControl> connectionControlMap = new HashMap<String, AbstractClientConnectionControl>();
		
		ServiceLoader<AbstractClientConnectionControl> connectionControls = ServiceLoader.load(AbstractClientConnectionControl.class);
		 for (AbstractClientConnectionControl connectionControl : connectionControls) {
			 connectionControlMap.put(connectionControl.getName(),connectionControl);
		 }
		
		AbstractClientConnectionControl connectionControl = connectionControlMap.get(schema);
		if (connectionControl == null) {
			throw new HippoClientException("could not find connectionControl, schema of type is ["+ schema +"]", 
					HippoCodeDefine.HIPPO_CLIENT_SPI_INIT_ERROR);
		}
		return connectionControl;
	}
	
	/**
	 * startup a listener for brokerUrl change in zookeeper
	 * @param connectionControl
	 */
	public static void startBrokerUrlListener(ClusterConnectionControl connectionControl) {
		ServiceLoader<StartupListener> startupListeners = ServiceLoader.load(StartupListener.class);
		for (StartupListener startupListener : startupListeners) {
				startupListener.startup(connectionControl);
		}
	}
}
