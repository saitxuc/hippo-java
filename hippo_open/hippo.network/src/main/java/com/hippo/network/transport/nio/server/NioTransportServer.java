package com.hippo.network.transport.nio.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.network.CommandManager;
import com.hippo.network.CommandResult;
import com.hippo.network.ServerFortress;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.RemoveConnectionCommand;
import com.hippo.network.command.RemoveSessionCommand;
import com.hippo.network.command.Response;
import com.hippo.network.command.SessionInfo;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.coder.CoderInitializer;
import com.hippo.network.transport.nio.coder.DefaultCoderInitializer;

/**
 * @author saitxuc
 * write 2014-7-17
 */
public class NioTransportServer extends LifeCycleSupport implements TransportServer {
	
	protected static final Logger LOG = LoggerFactory.getLogger(NioTransportServer.class);
	protected ServerBootstrap boot;
	protected Channel serverChannel; 
	protected EventLoopGroup bossGroup;
	protected EventLoopGroup workerGroup;
	protected int port = 61300;
	protected CommandManager commandManager;
	
	protected ServerFortress serverFortress;
	
	protected CoderInitializer coderInitializer = null;
	
	public NioTransportServer(int port, CommandManager commandManager) {
		this.port = port;
		this.commandManager = commandManager;
	}
	
	@Override
	public void doInit() {
		if(serverFortress == null) {
			throw new RuntimeException(" TransportServer do not set ServerFortress, cannot do work normally! ");
		}
		if(commandManager == null) {
			throw new RuntimeException(" TransportServer do not set commandManager, cannot do work normally! ");
		}
		if(coderInitializer == null) {
			coderInitializer = new DefaultCoderInitializer();
		}
		// Configure the server.
		NioServerDefaultHandler nethandler = new NioServerDefaultHandler(this);
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup();
		boot = new ServerBootstrap();
		boot.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_RCVBUF, 1024 * 1024)
				.childOption(
						ChannelOption.RCVBUF_ALLOCATOR,
						new AdaptiveRecvByteBufAllocator(1024, 1024 * 1024,
								10 * 1024 * 1024))
				.childHandler(new NioServerInitializer(nethandler, coderInitializer, true));
				

	}

	@Override
	public void doStart() {
		// Start the server.
		ChannelFuture f = null;
		try {
			f = boot.bind(port).sync();
			serverChannel = f.channel();
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public void doStop() {
		serverChannel.close();
		try{
			coderInitializer.close();
			// Wait until the server socket is closed.
			serverChannel.close();
			serverChannel.closeFuture().sync();
		}catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
		
	}
	
	@Override
	public void handleCommand(Object ctx, Command request) throws HippoException {
		String action = request.getAction();
		Response response = new Response();;
		if(CommandConstants.CONNECTION_INFO.equals(action)) {
			if(!(request instanceof ConnectionInfo)) {
				throw new HippoException("Command is not instance of ConnectionInfo.");
			}
			try{
				this.getServerFortress().getTransportConnectionManager().addConnectionInfo(ctx, (ConnectionInfo)request);
			}catch(Exception e) {
				throw new HippoException(e.getMessage());
			}
			
		}else if(CommandConstants.SESSION_INFO.equals(action)) {
			if(!(request instanceof SessionInfo)) {
				throw new HippoException("Command is not instance of SessionInfo.");
			}
			SessionInfo sessionInfo = (SessionInfo)request;
			this.getServerFortress().getTransportConnectionManager().addSessionInfoForConnection(sessionInfo.getConnectionId(), sessionInfo);
		}else if(CommandConstants.CONNECT_REMOVE_INFO.equals(action)) {
			if(!(request instanceof RemoveConnectionCommand)) {
				throw new HippoException("Command is not instance of RemoveConnectionCommand.");
			}
			this.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
			return;
		}else if(CommandConstants.SESION_REMOVE_INFO.equals(action)) {
			if(!(request instanceof RemoveSessionCommand)) {
				throw new HippoException("Command is not instance of RemoveSessionCommand.");
			}
			RemoveSessionCommand removeSessionCommand = (RemoveSessionCommand)request;
			this.getServerFortress().getTransportConnectionManager().removeSessionInfo(removeSessionCommand.getSessionId());
			return;
		}else {
			CommandResult cresult = this.commandManager.handleCommand(request, request.getAction());
			if(cresult != null) {
				response = assembleResponse(cresult);
			}
		}
		response.setAction(action);
		response.putHeadValue(CommandConstants.COMMAND_ID, request.getHeadValue(CommandConstants.COMMAND_ID));
		response.putHeadValue(CommandConstants.TYPE, CommandConstants.TYPE_RSP);
		if (request.getHeadValue(CommandConstants.KEY) != null) {
			response.putHeadValue(CommandConstants.KEY, request.getHeadValue(CommandConstants.KEY));
		}
		if(request.getHeadValue(CommandConstants.VERSION)!=null){
			response.putHeadValue(CommandConstants.VERSION, request.getHeadValue(CommandConstants.VERSION));
		}
		ChannelHandlerContext channelHandlerContext = (ChannelHandlerContext)ctx;
		channelHandlerContext.writeAndFlush(response);
	}
	
	@Override
	public Response assembleResponse(CommandResult cresult) {
		Response response = new Response();
		response.setContent(cresult.getMessage());
		response.setData(cresult.getData());
		response.setHeaders(cresult.getAttrMap());
		if(!cresult.isSuccess()){
			response.setFailure(true);
			response.setErrorCode(cresult.getErrorCode());
		}
		return response;
	}
	
	@Override
	public URI getConnectURI() {
		return null;
	}

	@Override
	public InetSocketAddress getSocketAddress() {
		return (InetSocketAddress)serverChannel.localAddress();
	}

	@Override
	public boolean isSslServer() {
		return false;
	}


	public void setServerChannel(Channel serverChannel) {
		this.serverChannel = serverChannel;
	}



	public void setCoderInitializer(CoderInitializer coderInitializer) {
		this.coderInitializer = coderInitializer;
	}

	@Override
	public ServerFortress getServerFortress() {
		return this.serverFortress;
	}

	@Override
	public void setServerFortress(ServerFortress serverFortress) {
		this.serverFortress = serverFortress;
	}

	
}
