package com.hippo.network.transport;

import com.hippo.common.exception.HippoException;
import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 * 2015-1-12
 */
public interface TransportListener<T> {
	
	/**
     * 
     * @param ctx
     * @param command
     * @throws HippoException
     */
    public void handleCommand(Object ctx, Command command) throws HippoException;
	
    /**
     * 
     * @param ctx
     * @throws HippoException
     */
    public void handleException(Object ctx) throws HippoException;
    
    /**
     * 
     * @param <T>
     * @throws HippoException
     */
    public void handleEvent(T eventtype) throws HippoException;
    
}
