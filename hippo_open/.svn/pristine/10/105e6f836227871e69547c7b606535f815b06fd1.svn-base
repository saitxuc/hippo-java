package com.pinganfu.hippo.network.transport.nio.client;

import java.util.concurrent.TimeUnit;

import com.pinganfu.hippo.network.transport.TransportListener;
import com.pinganfu.hippo.network.transport.nio.KeepAliveHandler;
import com.pinganfu.hippo.network.transport.nio.coder.CoderInitializer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * 
 * @author saitxuc
 * write 2014-7-1
 *
 */
public class NioClientInitializer extends ChannelInitializer<SocketChannel>  {
	
	private TransportListener transportListener;
	
	private CoderInitializer coderInitializer;
	
	public NioClientInitializer(TransportListener transportListener, CoderInitializer coderInitializer) {
		this.transportListener = transportListener;
		this.coderInitializer = coderInitializer;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.pipeline().addLast("encoder", coderInitializer.getEncoder());
		ch.pipeline().addLast("decoder", coderInitializer.getDecoder());
		ch.pipeline().addLast("keepAlive", new KeepAliveHandler(10, 30, TimeUnit.SECONDS, true));
		ch.pipeline().addLast("defaultHandler", new NioClientDefaultHandler(transportListener));
	}

}
