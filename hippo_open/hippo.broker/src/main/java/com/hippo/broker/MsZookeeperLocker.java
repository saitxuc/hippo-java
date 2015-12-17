package com.hippo.broker;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public class MsZookeeperLocker extends AbstractLocker {
	
	private static final Logger LOG = LoggerFactory.getLogger(MsZookeeperLocker.class);
	
	private String masterNode = null;
	
	private ZkClient zkClient;
	
	private String hostname;
	
	private final Object sleepMutex = new Object(); 
	
	@Override
	public void doInit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doStart() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doStop() {
		// TODO Auto-generated method stub
		
	}
	
}
