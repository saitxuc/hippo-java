package com.hippo.network.transport;

import java.io.IOException;
import java.net.URI;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.FutureResponse;
import com.hippo.network.ResponseCallback;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.coder.CoderInitializer;


/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public interface Transport extends LifeCycle {
	
	/**
	 * 
	 * @param command
	 * @throws IOException
	 */
    void oneway(Object command) throws IOException;
    
    /**
     * 
     * @param command
     * @param responseCallback
     * @return
     * @throws IOException
     */
    FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException;
    
    /**
     * 
     * @param command
     * @return
     * @throws IOException
     */
    Object request(Object command) throws IOException;

    /**
     * 
     * @param command
     * @param timeout
     * @return
     * @throws IOException
     */
    Object request(Object command, long timeout) throws IOException;

    
    /**
     * @return the remote address for this connection
     */
    String getRemoteAddress();

    /**
     * @return true if the transport is disposed
     */
    boolean isDisposed();

    /**
     * @return true if the transport is connected
     */
    boolean isConnected();

    /**
     * @return true if reconnect is supported
     */
    boolean isReconnectSupported();
    
    /**
     * 
     * @param listener
     */
    void setTransportListener(TransportListener listener);
    
    /**
     * 
     * @return
     */
    TransportListener getTransportListener();
    
    
    /**
     * 
     * @param ctx
     * @param command
     * @throws HippoException
     */
    public void onCommand(Object ctx, Command command) throws HippoException;
	
    /**
     * 
     * @param ctx
     * @throws HippoException
     */
    public void onChannelException(Object ctx) throws HippoException;
    
    /**
     * 
     * @param coderInitializer
     */
    public void setCoderInitializer(CoderInitializer coderInitializer);
    
}
