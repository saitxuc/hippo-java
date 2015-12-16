package com.pinganfu.hippo.network.transport.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;
import com.pinganfu.hippo.network.command.Response;


/**
 * 
 * @author saitxuc
 * write 2014-7-8
 */
public class ResponseFuture {
	
	final static Logger LOG = LoggerFactory.getLogger(ResponseFuture.class);

	private Channel channel;
	private static final Map<String, ResponseFuture> FUTURES = new ConcurrentHashMap<String, ResponseFuture>();
	private volatile Response response;
	private Object lock = new Object();
	private Command param;
	private long timeout = 10000;
	private final long start = System.currentTimeMillis();
	private final String txid;

	public boolean isDone() {
		return response != null;
	}

	public String getTxid() {
		return txid;
	}
	
	public ResponseFuture(Channel channel, Command param, long timeout) {
		this(channel, param);
		this.timeout = timeout;		
	}
	
	public ResponseFuture(Channel channel, Command param) {
		this.channel = channel;
		this.param = param;
		this.txid = param.getHeadValue(CommandConstants.COMMAND_ID);
	}

	public Response sendMsg(Command request) {

		//request.putHeadValue(CommandConstants.COMMAND_ID, txid);
		request.putHeadValue(CommandConstants.TYPE, CommandConstants.TYPE_REQ);
		request.putHeadValue(CommandConstants.VERSION, "1");

		FUTURES.put(txid, this);

		final CountDownLatch cd = new CountDownLatch(1);
		ChannelFuture cf = channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				cd.countDown();
			}
		});
		try {
			cd.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return this.get();

	}

	public Response get() {
		if (!isDone()) {
			synchronized (lock) {
				try {
					while (!isDone()) {
						lock.wait(timeout);
						if (isDone()) {
							return response;
						} else {
							Response temp = new Response();
							temp.setFailure(true);
							temp.setAction(param.getAction());
							temp.setContent("timeout");
							return temp;
						}
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			return response;
		} else {
			return response;
		}
	}

	public static void received(Response response) {
		String cid = response.getHeadValue(CommandConstants.COMMAND_ID);
		if(cid == null) {
			LOG.error("===>request is Illegal. response command id is null.");
			return;
		}
		ResponseFuture future = FUTURES.remove(cid);
		if (future != null) {
			future.doReceived(response);
		} else {
			LOG.error("===>received response error: " + response.getAction());
		}
		
	}

	private void doReceived(Response response) {

		synchronized (lock) {
			this.response = response;
			lock.notifyAll();
		}

	}

	private long getStartTimestamp() {
		return start;
	}

	public long getTimeout() {
		return timeout;
	}

	private static class RemotingInvocationTimeoutScan implements Runnable {

		public void run() {
			while (true) {
				try {
					for (ResponseFuture future : FUTURES.values()) {
						if (future == null || future.isDone()) {
							continue;
						}
						if (System.currentTimeMillis() - future.getStartTimestamp() > future.getTimeout()) {
							if(LOG.isErrorEnabled()) {
								LOG.error(" Command action : " + future.param.getAction() + " request timeout.");
							}
							Response command = new Response();
							command.setFailure(true);
							command.putHeadValue(CommandConstants.COMMAND_ID, future.getTxid());
							command.setContent("timeout");
							ResponseFuture.received(command);
						}
					}
					Thread.sleep(30);
				} catch (Throwable e) {
					LOG.error("Exception when scan the timeout invocation of remoting.", e);
				}
			}
		}
	}

	static {
		Thread th = new Thread(new RemotingInvocationTimeoutScan(), "HippoResponseTimeoutScanTimer");
		th.setDaemon(true);
		th.start();
	}
	
}
