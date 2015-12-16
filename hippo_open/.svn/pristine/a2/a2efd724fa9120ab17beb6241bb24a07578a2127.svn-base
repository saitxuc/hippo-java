package com.pinganfu.hippo.network;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.common.listener.EventListener;
import com.pinganfu.hippo.common.listener.ExceptionListener;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.ConnectionInfo;

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

