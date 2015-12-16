package com.pinganfu.hippoconsoleweb.lisneter;

import org.I0Itec.zkclient.IZkDataListener;

import com.pinganfu.hippoconsoleweb.zk.ZkRegisterService;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public class PrevLockListener implements IZkDataListener {

    private ZkRegisterService consoleService=null;
    public void handleDataChange(String dataPath, Object data) throws Exception {
    }

    public PrevLockListener(ZkRegisterService consoleService){
        this.consoleService = consoleService;
    }
    public void handleDataDeleted(String dataPath) throws Exception {
    	consoleService.resumeRegister();
    }
    
}
