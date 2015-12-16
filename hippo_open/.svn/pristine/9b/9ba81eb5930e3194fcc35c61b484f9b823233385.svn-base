package com.pinganfu.hippo.broker.cluster.zk;

import org.I0Itec.zkclient.IZkDataListener;

import com.pinganfu.hippo.broker.BrokerService;
import com.pinganfu.hippo.broker.cluster.simple.MsClusterBrokerService;
import com.pinganfu.hippo.broker.cluster.simple.ZkRegisterService;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public class PrevLockListener implements IZkDataListener {

    private ZkRegisterService brokerService=null;
    public void handleDataChange(String dataPath, Object data) throws Exception {
    }

    public PrevLockListener(ZkRegisterService brokerService){
        this.brokerService = brokerService;
    }
    public void handleDataDeleted(String dataPath) throws Exception {
    	brokerService.resumeRegister();
    }
    
}
