package com.pinganfu.hippo.network.transport;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.ConnectionInfo;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.nio.coder.CoderInitializer;

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
