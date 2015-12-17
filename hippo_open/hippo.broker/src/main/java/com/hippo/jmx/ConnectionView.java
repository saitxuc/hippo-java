package com.hippo.jmx;

import com.hippo.manager.ManagementContext;
import com.hippo.network.Connection;

/**
 * 
 * @author saitxuc
 *
 */
public class ConnectionView implements ConnectionViewMBean {
    
	private final ManagedConnection connection;
	
	private final ManagementContext managementContext;
	
	public ConnectionView(ManagedConnection connection) {
		this(connection, null);
	}
	
	public ConnectionView(ManagedConnection connection, ManagementContext managementContext) {
		this.connection = connection;
		this.managementContext = managementContext;
	}
	
	@Override
	public void init() {
		
	}

	@Override
	public void start() {
		connection.start();
	}

	@Override
	public void stop() {
		connection.stop();
	}

	@Override
	public boolean isStarted() {
		return connection.isStarted();
	}

	@Override
	public boolean isSlow() {
		//return connection.isSlow();
		return true;
	}

	@Override
	public boolean isBlocked() {
		// TODO Auto-generated method stub
		return connection.isBlock();
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return connection.isConnected();
	}

	@Override
	public boolean isActive() {
		// TODO Auto-generated method stub
		return connection.isActive();
	}

	@Override
	public void resetStatistics() {
		connection.getStatistics().reset();
	}

	@Override
	public String getRemoteAddress() {
		// TODO Auto-generated method stub
		//return connection.getRemoteAddress();
		return connection.getRemoteAddress();
	}

	@Override
	public String getClientId() {
		// TODO Auto-generated method stub
		return connection.getClientId();
	}

	@Override
	public String getUserName() {
		// TODO Auto-generated method stub
		return connection.getUserName();
	}

	@Override
	public int sessionCount() {
		// TODO Auto-generated method stub
		return connection.sessionCount();
	}

	@Override
	public Throwable getStartException() {
		// TODO Auto-generated method stub
		return connection.getStartException();
	}

}
