package com.hippo.broker.transport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.hippo.broker.BrokerService;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.jmx.ManagedConnection;
import com.hippo.jmx.ManagedTransportConnector;
import com.hippo.manager.ConnectorStatistics;
import com.hippo.manager.ManagementContext;
import com.hippo.network.ConnectionId;
import com.hippo.network.ServerFortress;
import com.hippo.network.ServerFortressFactory;
import com.hippo.network.impl.TransportServerFortressFactory;

/**
 * 
 * @author saitxuc
 * write 2014-8-7
 */
public class TransportConnector extends LifeCycleSupport implements Connector {
	
	private static final Logger LOG = LoggerFactory.getLogger(TransportConnector.class);
	
	private BrokerService brokerService;
	private ServerFortress serverFortress;
	private int bPort = -1;
	private String protocal;
	
	private String name;
	private boolean enableStatusMonitor = false;
	private final ConnectorStatistics statistics = new ConnectorStatistics();
	private int maximumConnections = Integer.MAX_VALUE;

	public TransportConnector() {
		super();
	}
	
	public TransportConnector(ServerFortress serverFortress) {
		this.serverFortress = serverFortress;
	}
	
	@Override
	public ConnectorStatistics getStatistics() {
		return statistics;
	}

	@Override
	public int connectionCount() {
		if(serverFortress == null) {
			return 0;
		}
		return serverFortress.getTransportConnectionManager().connectionCount();
	}

	@Override
	public void doInit() {
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector doing doInit. uri : " + this.getUri());
		}
		serverFortress.init();
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector finish doInit. uri : " + this.getUri());
		}
	}

	@Override
	public void doStart() {
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector doing doStart. uri : " + this.getUri());
		}
		serverFortress.start();
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector finish doStart. uri : " + this.getUri());
		}
	}

	@Override
	public void doStop() {
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector doing doStop. uri : " + this.getUri());
		}
		serverFortress.stop();
		if(LOG.isInfoEnabled()) {
			LOG.info(" TransportConnector finish doStop. uri : " + this.getUri());
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
    public ManagedTransportConnector asManagedConnector(ManagementContext context, ObjectName connectorName) throws HippoException {
        ManagedTransportConnector rc = new ManagedTransportConnector(context, connectorName, getServer());
        rc.setEnableStatusMonitor(isEnableStatusMonitor());
        //rc.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
        rc.setName(getName());
        rc.setMaximumConnections(maximumConnections);
        rc.setBrokerService(brokerService);
        return rc;
    }
	
    public ServerFortress getServer() throws HippoException {
       if (serverFortress == null) {
    	   serverFortress = createTransportFretressServer();
        }
        return serverFortress;
    }

	@Override
	public void setBrokerService(BrokerService brokerService) {
		this.brokerService = brokerService;
	}
    
    public boolean isEnableStatusMonitor() {
        return enableStatusMonitor;
    }
    
    
    public void setEnableStatusMonitor(boolean enableStatusMonitor) {
        this.enableStatusMonitor = enableStatusMonitor;
    }
    
    public String getUri() {
        return (this.protocal + "://" + bPort);
    }

    public ManagedConnection createConnection(ConnectionId connectionId, String clientIp, String clientId, String username) throws IOException{
    	return null;
    }
    
 // Implementation methods
    // -------------------------------------------------------------------------
    
    protected ServerFortress createTransportFretressServer() throws HippoException {
        if (bPort == -1) {
            throw new IllegalArgumentException("You must specify either a server or port property");
        }
        if(StringUtils.isEmpty(this.protocal)) {
        	throw new IllegalArgumentException("You must specify either a server or protocal property");
        }
        if (brokerService == null) {
            throw new IllegalArgumentException(
                    "You must specify the brokerService property. Maybe this connector should be added to a broker?");
        }
        ServerFortressFactory sfactory = new TransportServerFortressFactory(protocal, bPort, brokerService.getCommandManager(), new HippoTransportConnectionManager(this));
		ServerFortress serverFortress = sfactory.createServer();
		return serverFortress;
    }

	@Override
	public void setMaximumConnections(int count) {
		this.maximumConnections = count;
	}

	@Override
	public int getMaximumConnections() {
		return maximumConnections;
	}

	public void setServerFortress(ServerFortress serverFortress) {
		this.serverFortress = serverFortress;
	}

	public void setbPort(int bPort) {
		this.bPort = bPort;
	}

	public void setProtocal(String protocal) {
		this.protocal = protocal;
	}
	
}
