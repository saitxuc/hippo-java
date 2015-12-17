package com.hippo.network.transport.nio.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandManager;
import com.hippo.network.FutureResponse;
import com.hippo.network.ResponseCallback;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.Response;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportListener;
import com.hippo.network.transport.nio.HeartbeatTimestamp;
import com.hippo.network.transport.nio.coder.CoderInitializer;
import com.hippo.network.transport.nio.coder.DefaultCoderInitializer;

/**
 * @author saitxuc
 * write 2014-7-7 
 */
public class NioTransport extends LifeCycleSupport implements Transport {
	
	protected final static Logger LOG = LoggerFactory.getLogger(NioTransport.class);
	
	protected volatile boolean closed = false;
	protected final Object connecting = new Object();
	
	protected Bootstrap boot = null;
	
	protected CommandManager commandManager;
	
	private CoderInitializer coderInitializer = null;
	
	protected Channel channel;
	private EventLoopGroup group = new NioEventLoopGroup();
	protected String host;
	protected int port;
	protected TransportListener transportListener;
	
	public NioTransport(String host, int port) {
		this(host, port, null);
	}
	
	public NioTransport(String host, int port, CommandManager commandManager) {
		this.host = host;
		this.port = port;
		this.commandManager = commandManager;
	}
	
	@Override
	public void oneway(Object command) throws IOException {
		if(channel==null || !this.channel.isActive()) {
			throw new IOException("channel is not actived. ");
		}
		Command dc = (Command)command;
		channel.writeAndFlush(dc);
		HeartbeatTimestamp.setHeartbeatWriteTimestamp(channel);
	}

	@Override
	public FutureResponse asyncRequest(Object command,
			ResponseCallback responseCallback) throws IOException {
		throw new IllegalStateException("Not supported asyncRequest "); 
	}

	@Override
	public Object request(Object command) throws IOException {
		throw new IllegalStateException("Not supported asyncRequest "); 
	}

	@Override
	public Response request(Object command, long timeout) throws IOException {
		throw new IllegalStateException("Not supported asyncRequest "); 
	}

	@Override
	public String getRemoteAddress() {
		if (channel != null) {
			SocketAddress address = channel.remoteAddress();
            if (address instanceof InetSocketAddress) {
                return "tcp://" + ((InetSocketAddress)address).getAddress().getHostAddress() + ":" + ((InetSocketAddress)address).getPort();
            } else {
                return "" + channel.remoteAddress();
            }
        }
        return null;
	}

	@Override
	public boolean isDisposed() {
		return this.isConnected();
	}

	@Override
	public boolean isConnected() {
		if(channel == null) {
			return false;
		}
		return channel.isActive();
	}

	@Override
	public boolean isReconnectSupported() {
		return false;
	}



	@Override
	public void doInit() {
		try {
			if(coderInitializer == null) {
				coderInitializer = new DefaultCoderInitializer();
			}
			NioClientInitializer nioClientInitializer = new NioClientInitializer(transportListener, coderInitializer);
			boot = new Bootstrap();
			boot.option(ChannelOption.TCP_NODELAY, true);
			boot.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).
			option(ChannelOption.SO_RCVBUF, 1024 * 1024).
			option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(1024,1024*1024,10*1024*1024));
			boot.handler(nioClientInitializer);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(" NioTransport client do init happen fail. ", e);
		}
	}

	@Override
	public void doStart() {
		try{
			// Start the client.
			channel = boot.connect(host, port).sync().channel();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(" NioTransport do start happen error. ", e);
			
		}
	}
	
	
	@Override
	public void doStop() {
		try {
			coderInitializer.close();
			if(channel != null) {
				channel.close();
				channel.closeFuture().sync();
			}
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			group.shutdownGracefully();
		}
	}
	
	public void close() {
		stop();
	}


	public void onCommand(Object ctx, Command command)
			throws HippoException {
		if(commandManager != null) {
			this.commandManager.handleCommand(command);
		}else{
			LOG.warn(" commandManager is not set. no handle to do command. ");
		}
	}
	
	@Override
	public void onChannelException(Object ctx) throws HippoException {
	
	}
	
	public TransportListener getTransportListener() {
		return transportListener;
	}

	public void setTransportListener(TransportListener transportListener) {
		this.transportListener = transportListener;
	}

	public void setCoderInitializer(CoderInitializer coderInitializer) {
		this.coderInitializer = coderInitializer;
	}
	
}
