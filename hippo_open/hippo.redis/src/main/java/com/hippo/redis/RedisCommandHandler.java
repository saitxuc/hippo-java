package com.hippo.redis;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;
import com.hippo.network.ConnectionId;
import com.hippo.network.command.Command;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.transport.TransportConnectionManager;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.HeartbeatTimestamp;
import com.hippo.network.transport.nio.server.NioServerDefaultHandler;
import com.hippo.redis.command.RedisCommand;
import com.hippo.redis.util.BytesKey;

/**
 * 
 * @author saitxuc
 *
 */
public class RedisCommandHandler extends NioServerDefaultHandler {
	
	protected final static Logger LOG = LoggerFactory.getLogger(RedisCommandHandler.class);
	
	public RedisCommandHandler(TransportServer server) {
		super(server);
	}
	

	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		TransportConnectionManager connectionManager = this.server.getServerFortress().getTransportConnectionManager();
		if(connectionManager != null) {
			ConnectionInfo info = new ConnectionInfo();
			info.setClientIp(ctx.channel().remoteAddress().toString());
			ConnectionId cid = new ConnectionId(ctx.channel().remoteAddress().toString());
			info.setConnectionId(cid);
			connectionManager.addConnectionInfo(ctx, info);
		}
		//HeartbeatTimestamp.setHeartbeatReadTimestamp(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Command request = (Command) msg;
	    try{
			this.server.getServerFortress().getTransportConnectionManager().enStatics(ctx);
			this.server.handleCommand(ctx, request);
			this.server.getServerFortress().getTransportConnectionManager().deStatics(ctx);
		}catch(HippoException e) {
			LOG.error(e.getMessage(), e);
			Reply reply = new ErrorReply(e.getErrorCode());
			ctx.writeAndFlush(reply);
		}
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has unregister into eventloop. ");
		}
		TransportConnectionManager connectionManager = this.server.getServerFortress().getTransportConnectionManager();
		if(connectionManager != null) {
			this.server.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
		}
		
	}
	
}
