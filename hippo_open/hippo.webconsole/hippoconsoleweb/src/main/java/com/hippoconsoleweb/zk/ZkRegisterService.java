package com.hippoconsoleweb.zk;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;


/**
 * 
 * @author saitxuc
 * 2015-3-27
 */
public interface ZkRegisterService {
	
	/**
	 * 
	 */
	public void resumeRegister();
	
	/**
	 * 
	 * @return
	 */
	public String getClusterLockPath();
	
	/**
	 * 
	 * @return
	 */
	public String getMasterUrl();
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	boolean exists(String path);
	
	/**
	 * 
	 * @param path
	 * @param stateListener
	 */
	void subscribeDataChanges(String path,IZkDataListener stateListener);
	
	/**
	 * 
	 * @param stateListener
	 */
	void subscribeStateChanges(IZkStateListener stateListener);
	
	/**
	 * 
	 * @param url
	 * @param listener
	 */
	void unsubscribeData(String url, IZkDataListener listener);
	
	/**
	 * 
	 * @param stateListener
	 */
	void unsubscribeStateChanges(IZkStateListener stateListener);
	
}
