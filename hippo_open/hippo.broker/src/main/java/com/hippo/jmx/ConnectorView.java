package com.hippo.jmx;

import com.hippo.broker.transport.Connector;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public class ConnectorView implements ConnectorViewMBean {
	
	private Connector connector;
	
	public ConnectorView(Connector connector) {
		this.connector = connector;
	}
	
	public void start()  {
        connector.start();
    }

    public String getBrokerName() {
        //return getBrokerInfo().getBrokerName();
    	return null;
    }

    public void stop()  {
        connector.stop();
    }

	@Override
	public void init() {
		
	}

	@Override
	public boolean isStarted() {
		return connector.isStarted();
	}

	@Override
	public int connectionCount() {
		return connector.connectionCount();
	}

	@Override
	public void resetStatistics() {
		connector.getStatistics().reset();
	}

	@Override
	public void enableStatistics() {
		connector.getStatistics().setEnabled(true);
	}

	@Override
	public void disableStatistics() {
		connector.getStatistics().setEnabled(false);
	}

	@Override
	public boolean isStatisticsEnabled() {
		return connector.getStatistics().isEnabled();
	}

	@Override
	public Throwable getStartException() {
		// TODO Auto-generated method stub
		return connector.getStartException();
	}
	
    /***
    public BrokerInfo getBrokerInfo() {
        return connector.getBrokerInfo();
    }
    ***/
}
