package com.hippo.broker.cluster.zk;

import org.I0Itec.zkclient.IZkDataListener;

import com.hippo.broker.cluster.controltable.CtrlTableChangeManager;


/**
 * 
 * @author saitxuc
 *
 */
public class DtableChangeListener implements IZkDataListener {
	
	private CtrlTableChangeManager manager = null;
	
	public DtableChangeListener(CtrlTableChangeManager manager) {
		this.manager = manager;
	}
	
	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		/**
		 *  1.通过dtable算出mbucks和sbucks
		 *  2. 通过sbucks得出所要连接的masterurl
		 *  3 判断storeengine， inited， 1 否， 唤醒doinit方法，直接结束， 2 是， 根据新的sbucks信息， 重新整理ssrvice， 重置storeengine
		 * 
		 *  
		 */
		
		this.manager.handleDtableDataChange((String)data);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		//do nothing
	}

}
