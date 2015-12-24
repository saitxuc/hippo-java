package com.hippo.broker.transport;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.network.SessionId;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.SessionInfo;
import com.hippo.network.transport.nio.server.NioTransportConnectionManager;
import com.hippo.jmx.ManagedConnection;
/**
 * 
 * @author saitxuc
 * 2015-3-17
 */
public class HippoTransportConnectionManager extends NioTransportConnectionManager {
	
	 private static final Logger LOG = LoggerFactory.getLogger(HippoTransportConnectionManager.class);
	
	private TransportConnector connector;
	private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();
	private List<ManagedConnection> managedConnections = new ArrayList<ManagedConnection>();
	
	public HippoTransportConnectionManager(TransportConnector connector) {
		this.connector = connector;
		this.setMaximumConnections(connector.getMaximumConnections());
	}
	
	/**
	 * add by gusj
	 * @return
	 */
	public TransportConnector getConnector(){
		return this.connector;
	}
	
	/**
	 * add by gusj
	 * @return
	 */
	public List<ManagedConnection> getManagedConnections(){
		return this.managedConnections;
	}
	
	@Override
	public void addConnectionInfo(Object key, ConnectionInfo info) throws Exception {
		super.addConnectionInfo(key, info);
		ChannelHandlerContext ctx = (ChannelHandlerContext)key;
		info.setClientIp(ctx.channel().remoteAddress().toString());
		try {
			ManagedConnection managedConnection = connector.createConnection(info.getConnectionId(), info.getClientIp(), 
					info.getClientId(), info.getUserName());
			serviceLock.writeLock().lock();
            try {
            	managedConnections.add(managedConnection);
            } catch (Throwable e) {
                LOG.debug("Error occurred while add a managed connection " + this, e);
            } finally {
                serviceLock.writeLock().unlock();
            }
			
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public String removeConnectionInfo(Object key) {
		String connectionIds = super.removeConnectionInfo(key);
		if(StringUtils.isEmpty(connectionIds)) {
			return null;
		}
		serviceLock.writeLock().lock();
        try {
        	for(ManagedConnection managedConnection : managedConnections) {
        		if(connectionIds.equals(managedConnection.getConnectionIdString())) {
        			managedConnection.stop();
        			managedConnections.remove(managedConnection);
        			break;
        		}
        	}
        	
        } catch (Throwable e) {
            LOG.debug("Error occurred while add a managed connection " + this, e);
        } finally {
            serviceLock.writeLock().unlock();
        }
        
        return connectionIds;
	}

	@Override
	public String getConnectionId(Object key) {
		return super.getConnectionId(key);
	}

	@Override
	public void addSessionInfoForConnection(String connectionId,
			SessionInfo sessionInfo) {
		super.addSessionInfoForConnection(connectionId, sessionInfo);
		serviceLock.writeLock().lock();
        try {
        	for(ManagedConnection managedConnection : managedConnections) {
        		if(connectionId.equals(managedConnection.getConnectionIdString())) {
        			managedConnection.addSession();
        			break;
        		}
        	}
        	
        } catch (Throwable e) {
            LOG.debug("Error occurred while add a managed connection " + this, e);
        } finally {
            serviceLock.writeLock().unlock();
        }
	}

	@Override
	public void removeSessionInfo(SessionId sessionId) {
		super.removeSessionInfo(sessionId);
		serviceLock.writeLock().lock();
        try {
        	for(ManagedConnection managedConnection : managedConnections) {
        		if(sessionId.getConnectionId().equals(managedConnection.getConnectionIdString())) {
        			managedConnection.delSession();
        			break;
        		}
        	}
        	
        } catch (Throwable e) {
            LOG.debug("Error occurred while add a managed connection " + this, e);
        } finally {
            serviceLock.writeLock().unlock();
        }
	}

	@Override
	public void destroy() {
		super.destroy();
		managedConnections.clear();
	}

	@Override
	public int connectionCount() {
		// TODO Auto-generated method stub
		return currentTransportCount.get();
	}
	
	@Override
	public void enStatics(Object key) {
		String connectionIds = super.getConnectionId(key);
		if(StringUtils.isEmpty(connectionIds)) {
			return;
		}
		serviceLock.writeLock().lock();
        try {
        	for(ManagedConnection managedConnection : managedConnections) {
        		if(connectionIds.equals(managedConnection.getConnectionIdString())) {
        			managedConnection.getStatistics().getEnqueues().add(1);
        			break;
        		}
        	}
        	
        } catch (Throwable e) {
            LOG.debug("Error occurred while add a managed connection " + this, e);
        } finally {
            serviceLock.writeLock().unlock();
        }
	}

	@Override
	public void deStatics(Object key) {
		String connectionIds = super.getConnectionId(key);
		if(StringUtils.isEmpty(connectionIds)) {
			return;
		}
		serviceLock.writeLock().lock();
        try {
        	for(ManagedConnection managedConnection : managedConnections) {
        		if(connectionIds.equals(managedConnection.getConnectionIdString())) {
        			managedConnection.getStatistics().getDequeues().add(1);
        			break;
        		}
        	}
        	
        } catch (Throwable e) {
            LOG.debug("Error occurred while add a managed connection " + this, e);
        } finally {
            serviceLock.writeLock().unlock();
        }
	}
	
}
