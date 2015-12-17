package com.hippo.network;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.common.listener.EventListener;
import com.hippo.common.listener.ExceptionListener;
import com.hippo.network.command.Command;
import com.hippo.network.command.ConnectionInfo;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public interface Connection extends LifeCycle {
	
		Session createSession() throws HippoException;

	    String getClientID() throws HippoException;

	    void setClientID(String clientID) throws HippoException;

	    public void asyncSendPacket(Command command) throws HippoException;
	    
	    public Object syncSendPacket(Command command, long timeout) throws HippoException;
	    
	    void addExceptionListener(ExceptionListener listener);
	    
	    void addEventListener(EventListener listener);

	    ConnectionInfo getConnectionInfo();
	    
	    void close() throws HippoException;
	    
	    void stopAsync();
	    
	    
	    
}

