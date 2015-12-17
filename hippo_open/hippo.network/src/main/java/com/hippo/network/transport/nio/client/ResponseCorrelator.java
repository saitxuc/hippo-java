package com.hippo.network.transport.nio.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.network.FutureResponse;
import com.hippo.network.ResponseCallback;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.Response;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportFilter;

/**
 * 
 * @author saitxuc
 * 2015-1-12
 */
public class ResponseCorrelator extends TransportFilter {
	
	final static Logger LOG = LoggerFactory.getLogger(ResponseCorrelator.class);
	
	private static final String DEFAULT_ASNYC_COMMANDID = "-1";
	//private final int MAX_ENTRY = 10000;
	private final Map<String, FutureResponse> FUTURES = new ConcurrentHashMap<String, FutureResponse>();
	
	private UuidSequenceGenerator sequenceGenerator = new UuidSequenceGenerator();
	
	private ScheduledExecutorService scheduledExecutorService;
	
	public ResponseCorrelator(Transport next) {
		super(next);
	}
	
	@Override
	public void doInit() {
		Runnable task = new RemotingInvocationTimeoutScan();
		scheduledExecutorService = ExcutorUtils.startSchedule("HippoResponseTimeoutScanTimer ", task, 1000, 30);
		super.doInit();
	}

	
	@Override
	public void doStop() {
		if(scheduledExecutorService != null) {
			scheduledExecutorService.shutdown();
		}
		super.doStop();
	}
	
	public FutureResponse asyncRequest(Object command,
			ResponseCallback responseCallback) throws IOException {
		Command dc = (Command)command;
		String commandId = sequenceGenerator.getUuidNextSequence();
		/***
		synchronized(FUTURES) {
			if(FUTURES.size() >= MAX_ENTRY) {
				Response response = new Response();
				response.setFailure(true);
				response.putHeadValue(CommandConstants.COMMAND_ID, commandId);
				response.setContent("resquest map size is full.");
				FutureResponse future = new FutureResponse(dc, responseCallback);
				future.doReceived(response);
				return future;
			}
		}
		***/
		dc.putHeadValue(CommandConstants.COMMAND_ID, commandId);
		FutureResponse future = new FutureResponse(dc, responseCallback);
		FUTURES.put(commandId, future);
		next.oneway(command);
		return future;
	}
	
	public void oneway(Object command) throws IOException {
		Command dc = (Command)command;
		dc.putHeadValue(CommandConstants.COMMAND_ID, DEFAULT_ASNYC_COMMANDID);
		next.oneway(dc);
	}
	
	public Object request(Object command) throws IOException {
		FutureResponse response = asyncRequest(command, null);
        return response.getResult();
	}
	
	public Object request(Object command, long timeout) throws IOException {
		FutureResponse response = asyncRequest(command, null);
        return response.getResult(timeout);
	}
	
	public void onCommand(Object ctx, Command command) throws HippoException {
		String commandId = command.getHeadValue(CommandConstants.COMMAND_ID);
		if(DEFAULT_ASNYC_COMMANDID.equals(commandId)) {
			next.onCommand(ctx, command);
		}else{
			received((Response) command);
		}
	}
	
	private void received(Response response) {
		String cid = response.getHeadValue(CommandConstants.COMMAND_ID);
		if(cid == null) {
			LOG.error("===>request is Illegal. response command id is null.");
			return;
		}
		FutureResponse future = FUTURES.remove(cid);
		if (future != null) {
			future.doReceived(response);
		} else {
			LOG.error("===>received response error: " + response.getAction());
		}
	}
	
	
	private  class RemotingInvocationTimeoutScan implements Runnable {

		public void run() {
			//while (true) {
			try {
				for (FutureResponse future : FUTURES.values()) {
					if (future == null || future.isDone()) {
						continue;
					}
					if (System.currentTimeMillis() - future.getStartTimestamp() > future
							.getTimeout()) {
						if (LOG.isErrorEnabled()) {
							LOG.error(" Command action : " + future.getAction()
									+ " request timeout.");
						}
						Response command = new Response();
						command.setFailure(true);
						command.putHeadValue(CommandConstants.COMMAND_ID,
								future.getCommandId());
						command.setContent("timeout");
						ResponseCorrelator.this.received(command);
					}
				}
				// Thread.sleep(30);
			} catch (Throwable e) {
				LOG.error(
						"Exception when scan the timeout invocation of remoting.",
						e);
			}
			//}
		}
	}
	
}
