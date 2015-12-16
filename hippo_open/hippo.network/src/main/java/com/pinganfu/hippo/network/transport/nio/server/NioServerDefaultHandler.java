package com.pinganfu.hippo.network.transport.nio.server;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.TransportServer;
import com.pinganfu.hippo.network.transport.nio.HeartbeatTimestamp;

import io.netty.channel.ChannelHandler.Sharable;

/**
 * 
 * @author saitxuc
 *  rite 2014-7-1
 *
 */
@Sharable
public class NioServerDefaultHandler extends ChannelDuplexHandler {
	final static Logger LOG = LoggerFactory.getLogger(ChannelInboundHandlerAdapter.class);
	
	private TransportServer server;
	
	public NioServerDefaultHandler() {
		super();
	}
	
	public NioServerDefaultHandler(TransportServer server) {
		this.server = server;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		//this.server.addSession(session);
	    HeartbeatTimestamp.setHeartbeatReadTimestamp(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
	    HeartbeatTimestamp.setHeartbeatReadTimestamp(ctx);
	    Command request = (Command) msg;
		Response rsp = valicateRequest(request);
		if(rsp.isFailure()) {
			rsp.putHeadValue(CommandConstants.TYPE, CommandConstants.TYPE_RSP);
			if (request.getHeadValue(CommandConstants.KEY) != null) {
				rsp.putHeadValue(CommandConstants.KEY, request.getHeadValue(CommandConstants.KEY));
			}
			if(request.getHeadValue(CommandConstants.VERSION)!=null){
				rsp.putHeadValue(CommandConstants.VERSION, request.getHeadValue(CommandConstants.VERSION));
			}
			ctx.writeAndFlush(rsp);
			return;
		}
		try{
			this.server.getServerFortress().getTransportConnectionManager().enStatics(ctx);
			this.server.handleCommand(ctx, request);
			this.server.getServerFortress().getTransportConnectionManager().deStatics(ctx);
		}catch(HippoException e) {
			LOG.error(e.getMessage(), e);
			rsp.setFailure(true);
			rsp.setContent(e.getMessage());
			ctx.writeAndFlush(rsp);
		}

	}
	
	
	private Response valicateRequest(Command request) {
		Response rsp = new Response();
		if (request.getHeadValue(CommandConstants.COMMAND_ID) == null) {
			rsp.setContent(CommandConstants.RESPONSE_FAILURE);
			rsp.setFailure(true);
		}
		return rsp;
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has register into eventloop. ");
		}
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has unregister into eventloop. ");
		}
		this.server.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.warn("Unexpected exception from downstream.", cause);
		this.server.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
		ctx.close();
	}
	
	
	
}
