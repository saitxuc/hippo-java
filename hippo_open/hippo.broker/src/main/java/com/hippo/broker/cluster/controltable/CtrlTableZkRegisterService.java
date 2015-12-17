package com.hippo.broker.cluster.controltable;



/**
 * 
 * @author DPJ
 * 2015-3-27
 */
public interface CtrlTableZkRegisterService {
	
    public void reconnectCallback();
    
    public void disconnectCallback();
	
}
