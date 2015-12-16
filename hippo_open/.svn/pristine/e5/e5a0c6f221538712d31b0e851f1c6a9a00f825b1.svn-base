package com.pinganfu.hippoconsoleweb.lisneter;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippoconsoleweb.service.ConsoleManagerService;

/**
 * 
 * @author saitxuc
 * 2015-3-27
 */
public class ClusterResultListener implements IZkChildListener{
	
	private static final Logger LOG = LoggerFactory.getLogger(ClusterResultListener.class);
	
	private ConsoleManagerService console;
	
	public ClusterResultListener(ConsoleManagerService console) {
		this.console = console;
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
			    console.startSlave();
			}
		}
	}
}
