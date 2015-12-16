package com.pinganfu.hippo.broker.cluster.simple;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author saitxuc
 * 2015-3-27
 */
public class ClusterResultListener implements IZkChildListener{
	
	private static final Logger LOG = LoggerFactory.getLogger(ClusterResultListener.class);
	
	private MsClusterBrokerService clusterBrokerService;
	
	public ClusterResultListener(MsClusterBrokerService clusterBrokerService) {
		this.clusterBrokerService = clusterBrokerService;
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		if(LOG.isInfoEnabled()){
			LOG.info("  receive master node have create ");
		}
		if(currentChilds != null 
				&& currentChilds.size() == 1) {
			if("master".equals(currentChilds.get(0))) {
				clusterBrokerService.startSlave();
			}
		}
	}
}
