package com.pinganfu.hippo.network;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.network.transport.nio.coder.CoderInitializer;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public interface ConnectionFactory {
    
	/**
	 * 
	 * @param commandManager
	 */
	void setCommandManager(CommandManager commandManager);
	
	void setCoderInitializer(CoderInitializer coderInitializer);
	
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
	 * @return
	 * @throws HippoException
	 */
    Connection createConnection(String userName, String password)
        throws HippoException;
    
    /**
     * 
     * @param ntype
     */
    void setNioType(String ntype);
    
}
