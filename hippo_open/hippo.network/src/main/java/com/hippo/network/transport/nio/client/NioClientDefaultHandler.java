package com.hippo.network.transport.nio.client;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.listener.TransportEventEnum;
import com.hippo.network.command.Command;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportListener;

/**
 * 
 * @author saitxuc
 * write 2014-7-1
 *
 */
public class NioClientDefaultHandler extends ChannelDuplexHandler {
	
	private static final Logger LOG = LoggerFactory.getLogger(NioClientDefaultHandler.class);
	
	//private Transport transport;
	
	private TransportListener transportListener;
	
	public NioClientDefaultHandler() {
		super();
	}
	
	public NioClientDefaultHandler(TransportListener transportListener) {
		super();
		this.transportListener = transportListener;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has register into eventloop. ");
		}
		this.transportListener.handleEvent(TransportEventEnum.EVENT_REGISTER);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has unregister into eventloop. ");
		}
		transportListener.handleException(ctx);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has active into eventloop. ");
		}
		this.transportListener.handleEvent(TransportEventEnum.EVENT_ACTIVE);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if(LOG.isInfoEnabled()) {
			LOG.info(" this channel has unactive into eventloop. ");
		}
		this.transportListener.handleEvent(TransportEventEnum.EVENT_INACTIVED);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		transportListener.handleCommand(ctx, (Command)msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.warn("Unexpected exception from downstream.", cause);
		ctx.close();
	}
	
}
