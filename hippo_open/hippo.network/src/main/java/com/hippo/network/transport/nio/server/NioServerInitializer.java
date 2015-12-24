package com.hippo.network.transport.nio.server;

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import com.hippo.network.transport.nio.KeepAliveHandler;
import com.hippo.network.transport.nio.coder.CoderInitializer;

/**
 * 
 * @author saitxuc
 * write 2014-7-1
 */
public class NioServerInitializer extends ChannelInitializer<SocketChannel> {
	
	private NioServerDefaultHandler handler;
	
	private CoderInitializer<?> coderInitializer = null;
	
	private boolean keepalive = false;
	
	public NioServerInitializer(NioServerDefaultHandler handler, CoderInitializer<?> coderInitializer, boolean keepalive) {
		this.handler = handler;
		this.coderInitializer = coderInitializer;
		this.keepalive = keepalive;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.pipeline().addLast("encoder", coderInitializer.getEncoder());
		ch.pipeline().addLast("decoder", coderInitializer.getDecoder());
		if(keepalive) {
			ch.pipeline().addLast("keepAlive", new KeepAliveHandler(10, 30, TimeUnit.SECONDS, false));
		}
		ch.pipeline().addLast("defaultHandler", handler);
	}

}
