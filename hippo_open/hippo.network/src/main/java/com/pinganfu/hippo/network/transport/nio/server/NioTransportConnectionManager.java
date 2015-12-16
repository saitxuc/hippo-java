package com.pinganfu.hippo.network.transport.nio.server;

import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.NamedThreadFactory;
import com.pinganfu.hippo.network.SessionId;
import com.pinganfu.hippo.network.command.ConnectionInfo;
import com.pinganfu.hippo.network.command.SessionInfo;
import com.pinganfu.hippo.network.transport.ExceededMaximumConnectionsException;
import com.pinganfu.hippo.network.transport.TransportConnectionManager;
import com.pinganfu.hippo.network.transport.nio.HeartbeatTimestamp;

/**
 * @author saitxuc
 * write 2014-7-18
 */
public class NioTransportConnectionManager implements TransportConnectionManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(NioTransportConnectionManager.class);
    
    private final ScheduledExecutorService scheduled  = Executors.newScheduledThreadPool(1,new NamedThreadFactory("hippo-remoting-server-heartbeat",true));
    
    private Map<String, ChannelHandlerContext> remoteChannels = new ConcurrentHashMap<String, ChannelHandlerContext>();
    
    private Map<ChannelHandlerContext, String> ctxs = new ConcurrentHashMap<ChannelHandlerContext, String>();

    private final Map<String, ConnectionInfo> connections = new ConcurrentHashMap<String, ConnectionInfo>();

    private final Map<String, List<SessionInfo>> sessions = new ConcurrentHashMap<String, List<SessionInfo>>();
    
    private ScheduledFuture<?> heatbeatTimer;
    
    private int heartbeat;

    private int heartbeatTimeout;
    
    protected int maximumConnections = Integer.MAX_VALUE;
    protected AtomicInteger currentTransportCount = new AtomicInteger(0);
    
    public NioTransportConnectionManager() {
        this(10,30);
    }
    
    public NioTransportConnectionManager(int heartbeat,int heartbeatTimeout) {
        this.heartbeat = heartbeat;
        this.heartbeatTimeout = heartbeatTimeout;
        
        if (this.heartbeatTimeout < this.heartbeat * 2) {
            throw new IllegalStateException("heartbeatTimeout < heartbeatInterval * 2");
        }
        
        if (this.heartbeat > 0) {
        	//scheduled  = Executors.newScheduledThreadPool(1,new NamedThreadFactory("hippo-remoting-server-heartbeat",true));
        	startHeatbeatTimer();
        }
    }

    public Map<ChannelHandlerContext, String> getCtxs() {
        return ctxs;
    }

    public void setCtxs(Map<ChannelHandlerContext, String> ctxs) {
        this.ctxs = ctxs;
    }

    public void addClientContext(ChannelHandlerContext ctx, String connectionId) {
        ctxs.put(ctx, connectionId);
    }

    public String getClientId(ChannelHandlerContext ctx) {
        return ctxs.get(ctx);
    }

    public void addClientConnectionInfo(String connectionId, ConnectionInfo info) {
        connections.put(connectionId, info);
    }

    public ConnectionInfo getConnectionInfo(String connectionId) {
        return connections.get(connectionId);
    }

    @Override
    public synchronized void addConnectionInfo(Object key, ConnectionInfo info) throws Exception {
    	if (this.currentTransportCount.get() >= this.maximumConnections) {
            throw new ExceededMaximumConnectionsException("Exceeded the maximum " +
                "number of allowed client connections. See the 'maximumConnections' " +
                "property on the TCP transport configuration URI in the Hippo " +
                "configuration file (e.g., Hippo.xml/hippo.properties)");
    	}
		currentTransportCount.incrementAndGet();
		ChannelHandlerContext ctx = (ChannelHandlerContext) key;
		String remoteip = ctx.channel().remoteAddress().toString();
		remoteChannels.put(remoteip, ctx);
		ctxs.put(ctx, info.getConnectionId().getValue());
		connections.put(info.getConnectionId().getValue(), info);
    }
    
    @Override
    public synchronized void addSessionInfoForConnection(String connectionId, SessionInfo sessionInfo) {
        List<SessionInfo> sessionList = sessions.get(connectionId);
        if (sessionList == null) {
            sessionList = new ArrayList<SessionInfo>();
            sessions.put(connectionId, sessionList);
        }
        sessionList.add(sessionInfo);
    }

    @Override
    public synchronized String removeConnectionInfo(Object key) {
    	ChannelHandlerContext ctx = (ChannelHandlerContext) key;
		String remoteip = ctx.channel().remoteAddress().toString();
		remoteChannels.remove(remoteip);
    	String connectionId = ctxs.remove((ChannelHandlerContext) key);
        if(!StringUtils.isEmpty(connectionId)) {
        	connections.remove(connectionId);
            sessions.remove(connectionId);
        }
        currentTransportCount.decrementAndGet();
        return connectionId;
    }
    
    @Override
    public String getConnectionId(Object key) {
    	String connectionId = ctxs.get((ChannelHandlerContext) key);
    	return connectionId;
    }
    
    private void startHeatbeatTimer() {
        stopHeartbeatTimer();
        if (heartbeat > 0) {
            heatbeatTimer = scheduled.scheduleWithFixedDelay(
                    new ConnectionCheckTask(ctxs,TimeUnit.SECONDS.toNanos(heartbeatTimeout)),
                    heartbeat, heartbeat,TimeUnit.SECONDS);
        }
    }

    private void stopHeartbeatTimer() {
        try {
            ScheduledFuture<?> timer = heatbeatTimer;
            if (timer != null && ! timer.isCancelled()) {
                timer.cancel(true);
            }
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        } finally {
            heatbeatTimer = null;
        }
    }

    @Override
    public void destroy() {
        stopHeartbeatTimer();
        if(scheduled != null) {
        	scheduled.shutdown();
        }
        ctxs.clear();
        connections.clear();
        sessions.clear();
    }

    class ConnectionCheckTask implements Runnable {

        private Map<ChannelHandlerContext, String> contexts;

        private long heartbeatTimeout;

        public ConnectionCheckTask(Map<ChannelHandlerContext, String> contexts, long heartbeatTimeout) {
            super();
            this.contexts = contexts;
            this.heartbeatTimeout = heartbeatTimeout;
        }

        public void run() {
            if (contexts != null && contexts.size() > 0) {
                long now = System.nanoTime();
                for (Map.Entry<ChannelHandlerContext, String> entry : contexts.entrySet()) {
                    Long lastRead = HeartbeatTimestamp.getHeartbeatReadTimestamp(entry.getKey());
                    Long lastWrite = HeartbeatTimestamp.getHeartbeatWriteTimestamp(entry.getKey());
                    
                    if (lastRead != null && lastWrite != null && lastWrite > lastRead) {
                        lastRead = lastWrite;
                    }
                    if (lastRead != null && now - lastRead > heartbeatTimeout) {
                        NioTransportConnectionManager.this.removeConnectionInfo(entry.getKey());
                        if (entry.getKey().channel().isActive()) {
                            entry.getKey().channel().close();
                        }
                    }
                }
            }
        }
    }

	@Override
	public void removeSessionInfo(SessionId sessionId) {
		List<SessionInfo> sessionList = sessions.get(sessionId.getConnectionId());
        if (sessionList != null) {
        	for(SessionInfo sessionInfo : sessionList) {
        		if(sessionInfo.getSessionId().equals(sessionId)) {
        			sessionList.remove(sessionInfo);
        			return;
        		}
        	}
        }
        
	}
	
	@Override
	public ChannelHandlerContext getCtxByRemoteIp(String remoteIp) {
		return this.remoteChannels.get(remoteIp);
	}
	
	@Override
	public int connectionCount() {
		return connections.size();
	}

	@Override
	public void enStatics(Object key) {
		
	}

	@Override
	public void deStatics(Object key) {
		
	}

	@Override
	public void setMaximumConnections(int count) {
		this.maximumConnections = count;
	}

}
