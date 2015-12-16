package com.pinganfu.hippo.network;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;

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
