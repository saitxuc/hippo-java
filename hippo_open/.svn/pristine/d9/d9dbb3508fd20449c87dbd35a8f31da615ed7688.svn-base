package com.pinganfu.hippo.client.transport;

import com.pinganfu.hippo.client.ClientSessionResult;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.Session;

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
