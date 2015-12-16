package com.pinganfu.hippo.jmx;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.pinganfu.hippo.broker.transport.TransportConnector;
import com.pinganfu.hippo.manager.ManagementContext;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionId;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportServer;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public class ManagedTransportConnector extends TransportConnector {
	
static long nextConnectionId = 1;
    
    private final ManagementContext managementContext;
    private final ObjectName connectorName;

    public ManagedTransportConnector(ManagementContext context, ObjectName connectorName, ServerFortress serverFortress) {
        super(serverFortress);
        this.managementContext = context;
        this.connectorName = connectorName;
    }
    
    /**
     * add by gusj
     * @return
     */
    public ObjectName getConnectorName(){
    	return connectorName;
    }

    public ManagedTransportConnector asManagedConnector(MBeanServer mbeanServer, ObjectName connectorName) throws IOException, URISyntaxException {
        return this;
    }

    public ManagedConnection createConnection(ConnectionId connectionId, String clientIp, String clientId, String username) throws IOException {
        // prefer to use task runner from broker service as stop task runner, as we can then
        // tie it to the lifecycle of the broker service
        return new ManagedTransportConnection(this, connectionId,  clientIp, clientId, username, managementContext, connectorName);
    }

    protected static synchronized long getNextConnectionId() {
        return nextConnectionId++;
    }
	
}
