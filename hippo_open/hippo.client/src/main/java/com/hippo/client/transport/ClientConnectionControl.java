package com.hippo.client.transport;

import com.hippo.client.ClientSessionResult;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.Connection;
import com.hippo.network.Session;

/**
 * 
 * @author saitxuc
 * 2015-4-7
 */
public interface ClientConnectionControl extends LifeCycle {
	
	/**
	 * 
	 * @return
	 * @throws HippoException
	 */
	Connection createConnection() throws HippoException;
	
	/**
	 * 
	 * @param userName
	 * @param password
	 * @param brokerUrl
	 * @return
	 */
	Connection createConnection(String userName, String password, String brokerUrl) throws HippoException;
	
	/**
	 * 
	 * @return
	 */
	ClientSessionResult getSession(byte[] key) throws HippoException;
	
	/**
	 * 
	 * @param connectionId
	 * @param session
	 */
	void offerSession(byte[] key, Session session);
	
}
