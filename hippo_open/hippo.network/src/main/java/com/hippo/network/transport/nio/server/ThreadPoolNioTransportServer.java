package com.hippo.network.transport.nio.server;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.network.CommandManager;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.RemoveConnectionCommand;
import com.hippo.network.command.RemoveSessionCommand;
import com.hippo.network.command.Response;
import com.hippo.network.command.SessionInfo;

/**
 * 
 * @author saitxuc
 * 2015-4-29
 */
public class ThreadPoolNioTransportServer extends NioTransportServer {
	
	protected static final Logger LOG = LoggerFactory.getLogger(ThreadPoolNioTransportServer.class);
	
	private ExecutorService executorService = null;
	
	private int threadPoolSize = 50;
	
	public ThreadPoolNioTransportServer(int port, CommandManager commandManager) {
		super(port, commandManager);
	}
	
	@Override
	public void doInit() {
		super.doInit();
		executorService = ExcutorUtils.startPoolExcutor(threadPoolSize);
	}
	
	@Override
	public void doStop() {
		ExcutorUtils.shutdown(executorService);
		super.stop();
	}
	
	@Override
	public void handleCommand(final Object ctx, final Command request) throws HippoException {
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				Response response = new Response();
				try{
					String action = request.getAction();
					if(CommandConstants.CONNECTION_INFO.equals(action)) {
						if(!(request instanceof ConnectionInfo)) {
							LOG.error("Command is not instance of ConnectionInfo.");
							response.setFailure(true);
							response.setContent("Command is not instance of ConnectionInfo.");
						}else{
							try{
								ThreadPoolNioTransportServer.this.getServerFortress().getTransportConnectionManager().addConnectionInfo(ctx, (ConnectionInfo)request);
							}catch(Exception e) {
								LOG.error(e.getMessage());
								response.setFailure(true);
								response.setContent(e.getMessage());
							}
						}
					}else if(CommandConstants.SESSION_INFO.equals(action)) {
						if(!(request instanceof SessionInfo)) {
							LOG.error("Command is not instance of SessionInfo.");
							response.setFailure(true);
							response.setContent("Command is not instance of SessionInfo.");
						}else{
							SessionInfo sessionInfo = (SessionInfo)request;
							ThreadPoolNioTransportServer.this.getServerFortress().getTransportConnectionManager().addSessionInfoForConnection(sessionInfo.getConnectionId(), sessionInfo);
						}
					}else if(CommandConstants.CONNECT_REMOVE_INFO.equals(action)) {
						if(!(request instanceof RemoveConnectionCommand)) {
							LOG.error("Command is not instance of RemoveConnectionCommand.");
							response.setFailure(true);
							response.setContent("Command is not instance of RemoveConnectionCommand.");
						}else{
							ThreadPoolNioTransportServer.this.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
							return;
						}
					}else if(CommandConstants.SESION_REMOVE_INFO.equals(action)) {
						if(!(request instanceof RemoveSessionCommand)) {
							LOG.error("Command is not instance of RemoveSessionCommand.");
							response.setFailure(true);
							response.setContent("Command is not instance of RemoveSessionCommand.");
						}else{
							RemoveSessionCommand removeSessionCommand = (RemoveSessionCommand)request;
							ThreadPoolNioTransportServer.this.getServerFortress().getTransportConnectionManager().removeSessionInfo(removeSessionCommand.getSessionId());
							return;
						}
					}else {
						CommandResult cresult = ThreadPoolNioTransportServer.this.commandManager.handleCommand(request);
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
				}catch(Exception e) {
					LOG.error(e.getMessage(), e);
					response.setFailure(true);
					response.setContent(e.getMessage());
					ChannelHandlerContext channelHandlerContext = (ChannelHandlerContext)ctx;
					channelHandlerContext.writeAndFlush(response);
				}
				
			}
			
		});
	}
	
}
