package com.pinganfu.hippo.network.transport;

import io.netty.channel.ChannelHandlerContext;

import com.pinganfu.hippo.network.SessionId;
import com.pinganfu.hippo.network.command.ConnectionInfo;
import com.pinganfu.hippo.network.command.SessionInfo;

/**
 * 
 * @author saitxuc
 * write 2014-7-21
 */
public interface TransportConnectionManager {
	
	public void addConnectionInfo(Object key, ConnectionInfo info) throws Exception;
	
	public String removeConnectionInfo(Object key);
	
	public String getConnectionId(Object key);
	
	public void addSessionInfoForConnection(String connectionId, SessionInfo sessionInfo);
	
	public void removeSessionInfo(SessionId sessionId);
	
	public void destroy();
	
	public int connectionCount();
	
	public void enStatics(Object key);
	
	public void deStatics(Object key);
	
	void setMaximumConnections(int count);
	
	ChannelHandlerContext getCtxByRemoteIp(String remoteIp);
	
}
