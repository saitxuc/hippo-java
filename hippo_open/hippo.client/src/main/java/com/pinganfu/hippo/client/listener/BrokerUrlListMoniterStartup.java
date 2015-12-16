package com.pinganfu.hippo.client.listener;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.client.transport.cluster.ClusterConnectionControl;
import com.pinganfu.hippo.client.transport.simple.SimpleConnectionControl;
import com.pinganfu.hippo.client.util.ZkUtil;
import com.pinganfu.hippo.common.ZkConstants;

public class BrokerUrlListMoniterStartup implements StartupListener{
	private static final Logger log = LoggerFactory.getLogger(SimpleConnectionControl.class);
	
	@Override
	public void startup(ClusterConnectionControl clusterControl) {
		//subscribe brokerUrl list change
		ZkClient zkClient = ZkUtil.getZKClient(clusterControl.getZookeeperUrl());
		BrokerUrlListChangeListener listener = new BrokerUrlListChangeListener(clusterControl);
		try {
			zkClient.subscribeDataChanges(ZkConstants.TABLES + ZkConstants.NODE_CTABLE, listener);
			log.info("Subscribe ctable's data change success: {}", ZkConstants.TABLES + ZkConstants.NODE_CTABLE);
		} catch (Exception e) {
			log.error("subscribe ctable's data change error. ");
		}
	}

}
