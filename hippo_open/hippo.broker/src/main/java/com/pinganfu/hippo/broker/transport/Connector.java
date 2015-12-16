package com.pinganfu.hippo.broker.transport;

import com.pinganfu.hippo.broker.BrokerService;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.manager.ConnectorStatistics;

/**
 * 
 * @author saitxuc
 * write 2014-8-8
 */
public interface Connector extends LifeCycle {
	
	//BrokerInfo getBrokerInfo();

    /**
     * @return the statistics for this connector
     */
    ConnectorStatistics getStatistics();
    
    /**
     * 
     * @return
     */
    int connectionCount();
    
    /**
     * 
     * @param brokerService
     */
    void setBrokerService(BrokerService brokerService);
	
    /**
     * 
     * @param count
     */
    void setMaximumConnections(int count);
    
    /**
     * 
     * @return
     */
    int getMaximumConnections();
}
