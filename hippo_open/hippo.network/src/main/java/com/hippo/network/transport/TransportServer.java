package com.hippo.network.transport;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.CommandResult;
import com.hippo.network.Connection;
import com.hippo.network.ServerFortress;
import com.hippo.network.command.Command;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.coder.CoderInitializer;

/**
 * 
 * @author saitxuc
 * write 2014-7-17
 */
public interface TransportServer extends LifeCycle {
	
	/**
	 * 
	 */
	ServerFortress getServerFortress();
	
	/**
	 * 
	 */
	void setServerFortress(ServerFortress serverFortress);
	
    /**
     * 
     * @param brokerInfo
     */
	//void setBrokerInfo(BrokerInfo brokerInfo);
	
	/**
	 * 
	 * @return
	 */
    URI getConnectURI();
    
    /**
     * 
     * @return
     */
    InetSocketAddress getSocketAddress();

    /**
     * 
     * @return
     */
    boolean isSslServer();
    
    /**
     * 
     * @param command
     */
    public void handleCommand(Object ctx, Command command) throws HippoException;
    
    /**
     * 
     * @param coderInitializer
     */
    void setCoderInitializer(CoderInitializer coderInitializer);
    
    /**
     * 
     * @return
     */
    Response assembleResponse(CommandResult cresult);
}
