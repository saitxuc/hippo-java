package com.hippo.network;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public interface Session extends LifeCycle {
	/**
	 * 
	 */
	public void reset();
	
	/**
	 * 
	 * @throws HippoException
	 */
	public void close() throws HippoException;
	
	/**
	 * 
	 * @param data
	 * @param timeout
	 * @return
	 * @throws HippoException
	 */
	public Response send(Command data, long timeout) throws HippoException; 
	
	/**
	 * 
	 * @param data
	 * @throws HippoException
	 */
	public void asnysend(Command data) throws HippoException; 
	

}
