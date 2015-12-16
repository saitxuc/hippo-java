package com.pinganfu.hippo.jmx;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.transport.TransportConnector;
import com.pinganfu.hippo.common.IdGenerator;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.listener.ExceptionListener;
import com.pinganfu.hippo.common.util.IOExceptionSupport;
import com.pinganfu.hippo.manager.ConnectionStatistics;
import com.pinganfu.hippo.manager.ManagementContext;
import com.pinganfu.hippo.network.ConnectionId;
import com.pinganfu.hippo.network.Session;
import com.pinganfu.hippo.network.command.ConnectionInfo;
import com.pinganfu.hippo.network.impl.TransportConnection;
import com.pinganfu.hippo.network.transport.Transport;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public class ManagedTransportConnection extends LifeCycleSupport implements ManagedConnection {
	
	protected static final Logger LOG = LoggerFactory.getLogger(ManagedTransportConnection.class);
	
	private final ManagementContext managementContext;
    private final ObjectName connectorName;
    private final ConnectionViewMBean mbean;

    private ObjectName byClientIdName;
    private ObjectName byAddressName;
    
    private TransportConnector connector;
    private ConnectionId connectionId;
    private final ConnectionStatistics statistics = new ConnectionStatistics();
    private boolean actived = true;
    private boolean blocked = false;
    private boolean connected = true;
    private boolean slowed = false;
    private String remoteAddress;
    private String clientId;
    private String username;
    private AtomicInteger sessionCount = new AtomicInteger(0);
    //private final boolean populateUserName;
	
    public ManagedTransportConnection(TransportConnector connector,
    		ConnectionId connectionId,
    		String clientIp,
    		String clientId,
    		String username,
			ManagementContext managementContext,
			ObjectName connectorName
			)
			throws IOException {
		this.connector = connector;
		this.connectionId = connectionId;
		this.managementContext = managementContext;
        this.connectorName = connectorName;
        remoteAddress = clientIp;
        this.clientId = clientId;
        this.username = username;
        this.mbean = new ConnectionView(this, managementContext);
        if (managementContext.isAllowRemoteAddressInMBeanNames()) {
            byAddressName = createObjectName("remoteAddress", clientIp);
            registerMBean(byAddressName);
        }
        
        if(!StringUtils.isEmpty(this.clientId)) {
        	byClientIdName = createObjectName("clientId", this.clientId);
        	registerMBean(byClientIdName);
        }
        statistics.setParent(this.connector.getStatistics());
    }
    
    
   public void stopAsync() {
        if(this.isStopped()) {
            synchronized (this) {
            	doStop();
            }
        }
     }
    
	@Override
	public ConnectionStatistics getStatistics() {
		return statistics;
	}
	
	public void setConnector(TransportConnector connector) {
		this.connector = connector;
	}

	public void destroys() {
		if(byAddressName != null) {
			unregisterMBean(byAddressName);
		}
		if(byClientIdName != null) {
			unregisterMBean(byClientIdName);
		}
		
	}
	// Implementation methods
    // -------------------------------------------------------------------------
    protected void registerMBean(ObjectName name) {
        if (name != null) {
            try {
                AnnotatedMBean.registerMBean(managementContext, mbean, name);
            } catch (Throwable e) {
                LOG.warn("Failed to register MBean: " + name);
                LOG.debug("Failure reason: " + e, e);
            }
        }
    }

    protected void unregisterMBean(ObjectName name) {
        if (name != null) {
            try {
                managementContext.unregisterMBean(name);
            } catch (Throwable e) {
                LOG.warn("Failed to unregister mbean: " + name);
                LOG.debug("Failure reason: " + e, e);
            }
        }
    }

    protected ObjectName createObjectName(String type, String value) throws IOException {
        try {
            return BrokerMBeanSupport.createConnectionViewByType(connectorName, type, value);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

	@Override
	public void doInit() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void doStart() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void doStop() {
		synchronized (this) {
            unregisterMBean(byClientIdName);
            unregisterMBean(byAddressName);
            byClientIdName = null;
            byAddressName = null;
        }
	}


	public ConnectionId getConnectionId() {
		return connectionId;
	}


	@Override
	public String getConnectionIdString() {
		return connectionId.getValue();
	}


	@Override
	public boolean isActive() {
		// TODO Auto-generated method stub
		return actived;
	}


	@Override
	public boolean isBlock() {
		// TODO Auto-generated method stub
		return blocked;
	}


	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return connected;
	}


	@Override
	public boolean isSlow() {
		// TODO Auto-generated method stub
		return slowed;
	}


	@Override
	public String getRemoteAddress() {
		// TODO Auto-generated method stub
		return remoteAddress;
	}


	@Override
	public String getClientId() {
		// TODO Auto-generated method stub
		return clientId;
	}


	@Override
	public String getUserName() {
		// TODO Auto-generated method stub
		return username;
	}


	@Override
	public int sessionCount() {
		// TODO Auto-generated method stub
		return sessionCount.get();
	}


	@Override
	public void addSession() {
		sessionCount.incrementAndGet();
	}


	@Override
	public void delSession() {
		sessionCount.decrementAndGet();
	}
	
	/**
	 * add by gusj
	 * @return
	 */
	public String getByAddressName(){
		return byAddressName.toString();
	}
    
}
