package com.hippo.broker.cluster.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.cluster.controltable.CtrlTableZkRegisterService;

/**
 * 
 * @author DPJ
 */
public class CtrlTableStateListenerImpl extends StateListenerImpl {
	private static final Logger LOG = LoggerFactory.getLogger(CtrlTableStateListenerImpl.class);
	
    private CtrlTableZkRegisterService registerService = null;

    public CtrlTableStateListenerImpl(CtrlTableZkRegisterService registerService) {
        super(null);
        
        this.registerService = registerService;
    }

    @Override
    public void handleNewSession() throws Exception {
        LOG.info(" handle new session ");
        try {
            registerService.reconnectCallback();
        } catch (Exception e) {
            LOG.error(" server reconnect callback happen error! ");
        }
    }

    @Override
    public void stopMaster() {
        try {
            registerService.disconnectCallback();
        } catch (Exception e) {
            LOG.error(" server disconnect callback happen error! ");
        }
    }
}
