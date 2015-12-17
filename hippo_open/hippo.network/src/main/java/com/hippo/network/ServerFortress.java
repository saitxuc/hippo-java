package com.hippo.network;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.transport.TransportConnectionManager;
import com.hippo.network.transport.TransportServer;

/**
 * 
 * @author saitxuc
 * 2015-3-30
 */
public interface ServerFortress extends LifeCycle {
	
	/**
	 * 
	 * @return
	 */
	String getSchema();
	
	/**
	 * 
	 * @return
	 */
	TransportServer getTransportServer();
	
	/** 
	 * 
	 * @throws HippoException
	 */
	void close() throws HippoException;
	
    /**
     * 
     * @param manager
     */
    void setTransportConnectionManager(TransportConnectionManager manager);
    
    /**
     * 
     */
    TransportConnectionManager getTransportConnectionManager();
    
}
