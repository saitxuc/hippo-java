package com.pinganfu.hippo.broker.cluster.zk;

import org.I0Itec.zkclient.IZkDataListener;

import com.pinganfu.hippo.broker.cluster.controltable.CtrlTableChangeManager;


/**
 * 
 * @author saitxuc
 *
 */
public class MtableChangeListener implements IZkDataListener {
	
	private CtrlTableChangeManager manager = null;
	
	public MtableChangeListener(CtrlTableChangeManager manager) {
		this.manager = manager;
	}
	
	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		/**
		 * 当mtable和dtable相同时， 1. 没有started时， 唤醒dostart， 
		 * 完成后注销mtable的订阅
		 * 
		 */
		this.manager.handleMtableDataChange((String)data);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		//do nothing
	}

}
