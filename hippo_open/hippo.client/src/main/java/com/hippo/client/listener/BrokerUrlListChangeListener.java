package com.hippo.client.listener;

import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.client.transport.cluster.ClusterConnectionControl;

public class BrokerUrlListChangeListener implements IZkDataListener {
	private static final Logger log = LoggerFactory.getLogger(BrokerUrlListChangeListener.class);
	
	private ClusterConnectionControl clusterControl;
	
	public BrokerUrlListChangeListener(ClusterConnectionControl connectionManager) {
		this.clusterControl = connectionManager;
	}
	
	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		log.info("handle data change ...");
		clusterControl.disposeBrokerUrlListChange((String)data);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		

	}

}
