package com.pinganfu.hippo.network;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.network.transport.TransportConnectionManager;

/**
 * 
 * @author saitxuc
 * 2015-3-30
 */
public interface ServerFortressFactory {
	
	/**
	 * 
	 * @param connectionManager
	 */
	void setConnectionManager(TransportConnectionManager connectionManager);
	
	/**
	 * 
	 * @param commandManager
	 */
	void setCommandManager(CommandManager commandManager);
	
	/**
	 * 
	 * @return
	 * @throws HippoException
	 */
	ServerFortress createServer() throws HippoException;
	
	/**
	 * 
	 * @param bindPort
	 * @param sheme
	 * @param commandManager
	 * @return
	 * @throws HippoException
	 */
	ServerFortress createServer(int bindPort, String sheme, CommandManager commandManager, TransportConnectionManager connectionManager) throws HippoException;
	
	/**
	 * 
	 * @param ntype
	 */
	void setNioType(String ntype);
	
	
	
}
