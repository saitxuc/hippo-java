package com.hippo.network.transport.nio.server;

import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.hippo.common.exception.HippoException;
import com.hippo.network.CommandManager;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.RemoveConnectionCommand;
import com.hippo.network.command.RemoveSessionCommand;
import com.hippo.network.command.Response;
import com.hippo.network.command.SessionInfo;
import com.hippo.network.disruptor.DisruptorExecuteMessage;
import com.hippo.network.disruptor.DisruptorExecutor;
import com.hippo.network.disruptor.DisruptorHandle;
import com.hippo.network.disruptor.DisruptorQueue;
import com.hippo.network.disruptor.DisruptorUtils;
import com.hippo.network.disruptor.callback.AsyncLoopThread;

/**
 * 
 * @author saitxuc
 *
 */
public class DisruptorNioTransportServer extends NioTransportServer implements DisruptorHandle {
	
	protected static final Logger LOG = LoggerFactory.getLogger(DisruptorNioTransportServer.class);
	
	private DisruptorQueue exeQueue  = null;
	
	private int threadNum = 15;
	private List<AsyncLoopThread> allThreads = new ArrayList<AsyncLoopThread>();
	
	public DisruptorNioTransportServer(int port, CommandManager commandManager) {
		super(port, commandManager);
	}
	
	@Override
	public void doInit() {
		
		WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(10, TimeUnit.MILLISECONDS);
		exeQueue =
                DisruptorQueue.mkInstance("DisruptorExe", ProducerType.SINGLE,
                        2048, waitStrategy);
		
		exeQueue.consumerStarted();
		
		for(int i = 0; i < threadNum; i++) {
			DisruptorExecutor baseExecutor = new DisruptorExecutor(exeQueue, this);
			AsyncLoopThread executor_threads =
	                new AsyncLoopThread(baseExecutor, false, Thread.MAX_PRIORITY,
	                        true);
			allThreads.add(executor_threads);
		}
		super.doInit();
	}
	
	@Override
	public void doStop() {
		
        for (AsyncLoopThread thr : allThreads) {
            LOG.info("Begin to shutdown " + thr.getThread().getName());
            thr.cleanup();
            DisruptorUtils.sleepMs(10);
            thr.interrupt();
            LOG.info("Successfully shutdown " + thr.getThread().getName());
        }
		
		super.stop();
	}
	
	@Override
	public void handleCommand(final Object ctx, final Command request) throws HippoException {
		DisruptorExecuteMessage disexe = new DisruptorExecuteMessage(ctx, request);
		exeQueue.publish(disexe);
	}
	
	
	
	public void handEvent(DisruptorExecuteMessage disexe) {
		Object ctx = disexe.getCtx();
		Command request = disexe.getRequest();
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
						DisruptorNioTransportServer.this.getServerFortress().getTransportConnectionManager().addConnectionInfo(ctx, (ConnectionInfo)request);
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
					DisruptorNioTransportServer.this.getServerFortress().getTransportConnectionManager().addSessionInfoForConnection(sessionInfo.getConnectionId(), sessionInfo);
				}
			}else if(CommandConstants.CONNECT_REMOVE_INFO.equals(action)) {
				if(!(request instanceof RemoveConnectionCommand)) {
					LOG.error("Command is not instance of RemoveConnectionCommand.");
					response.setFailure(true);
					response.setContent("Command is not instance of RemoveConnectionCommand.");
				}else{
					DisruptorNioTransportServer.this.getServerFortress().getTransportConnectionManager().removeConnectionInfo(ctx);
					return;
				}
			}else if(CommandConstants.SESION_REMOVE_INFO.equals(action)) {
				if(!(request instanceof RemoveSessionCommand)) {
					LOG.error("Command is not instance of RemoveSessionCommand.");
					response.setFailure(true);
					response.setContent("Command is not instance of RemoveSessionCommand.");
				}else{
					RemoveSessionCommand removeSessionCommand = (RemoveSessionCommand)request;
					DisruptorNioTransportServer.this.getServerFortress().getTransportConnectionManager().removeSessionInfo(removeSessionCommand.getSessionId());
					return;
				}
			}else {
				CommandResult cresult = DisruptorNioTransportServer.this.commandManager.handleCommand(request, request.getAction());
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
	
}
