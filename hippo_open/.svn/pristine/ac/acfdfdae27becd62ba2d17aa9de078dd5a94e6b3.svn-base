package com.pinganfu.hippo.network.transport.nio;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.EventExecutor;

/**
 * @author saitxuc
 * write 2014-7-2
 */
public class KeepAliveHandler extends ChannelDuplexHandler{
	
	protected static final Logger LOG = LoggerFactory.getLogger(KeepAliveHandler.class); 
	
	private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

	private final long readerIdleTimeNanos;
	// keepAlive timeout
	private final long readerIdleTimeKeepAliveTimeOutNanos;

	private AtomicBoolean isReaderIdel = new AtomicBoolean(false);

	volatile ScheduledFuture<?> readerIdleTimeout;
	
	volatile long lastReadTime;

	private volatile int state; // 0 - none, 1 - initialized, 2 - destroyed
	
	private long nextDelayLimit;
	
	private boolean isClient;
	
	/**
	 * Creates a new instance firing {@link IdleStateEvent}s.
	 * 
	 * @param readerIdleTime
	 *            an {@link IdleStateEvent} whose state is
	 *            {@link IdleState#READER_IDLE} will be triggered when no read
	 *            was performed for the specified period of time. Specify
	 *            {@code 0} to disable.
	 * @param writerIdleTime
	 *            an {@link IdleStateEvent} whose state is
	 *            {@link IdleState#WRITER_IDLE} will be triggered when no write
	 *            was performed for the specified period of time. Specify
	 *            {@code 0} to disable.
	 * @param allIdleTime
	 *            an {@link IdleStateEvent} whose state is
	 *            {@link IdleState#ALL_IDLE} will be triggered when neither read
	 *            nor write was performed for the specified period of time.
	 *            Specify {@code 0} to disable.
	 * @param unit
	 *            the {@link TimeUnit} of {@code readerIdleTime},
	 *            {@code writeIdleTime}, and {@code allIdleTime}
	 */
	public KeepAliveHandler(long readerIdleTime, long readerIdleTimeKeepAliveTimeOut, TimeUnit unit,boolean isClient) {
		if (unit == null) {
			throw new NullPointerException("unit");
		}

		this.isClient = isClient;
		
		if (readerIdleTime <= 0) {
			readerIdleTimeNanos = 0;
		} else {
			readerIdleTimeNanos = Math.max(unit.toNanos(readerIdleTime), MIN_TIMEOUT_NANOS);
		}

		if (readerIdleTime <= 0) {
			readerIdleTimeKeepAliveTimeOutNanos = 0;
		} else {
			readerIdleTimeKeepAliveTimeOutNanos = Math.max(unit.toNanos(readerIdleTimeKeepAliveTimeOut), MIN_TIMEOUT_NANOS);
		}
		nextDelayLimit = readerIdleTimeNanos * 2;
	}
	
	public long getReaderIdleTimeInMillis() {
		return TimeUnit.NANOSECONDS.toMillis(readerIdleTimeNanos);
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
			// channelActvie() event has been fired already, which means
			// this.channelActive() will
			// not be invoked. We have to initialize here instead.
			initialize(ctx);
		} else {
			// channelActive() event has not been fired yet.
			// this.channelActive() will be invoked
			// and initialization will occur there.
		}
	}
	
	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		destroy();
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		// Initialize early if channel is active already.
		if (ctx.channel().isActive()) {
			initialize(ctx);
		}
		super.channelRegistered(ctx);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// This method will be invoked only if this handler was added
		// before channelActive() event is fired. If a user adds this handler
		// after the channelActive() event, initialize() will be called by
		// beforeAdd().
		initialize(ctx);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		destroy();
		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
	    HeartbeatTimestamp.setHeartbeatReadTimestamp(ctx);
		lastReadTime = System.nanoTime();
		
		// test
		Command temp = (Command) msg;
		
		if(temp.getAction().startsWith(CommandConstants.KEEPALIVE)){
			if( !isClient && CommandConstants.ALIVE_MSG_CONTENT.equals(temp.getHeadValue(CommandConstants.ALIVE_MSG))){
				Command rsp = new Command();
				rsp.setAction(CommandConstants.KEEPALIVE);
				rsp.putHeadValue(CommandConstants.ALIVE_MSG, CommandConstants.ALIVE_MSG_CONTENT);
				ctx.writeAndFlush(rsp);
			}
		} else{
			ctx.fireChannelRead(msg);
		}
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		ctx.write(msg, promise);
	}

	private void initialize(ChannelHandlerContext ctx) {
		// Avoid the case where destroy() is called before scheduling timeouts.
		// See: https://github.com/netty/netty/issues/143
	    if (!isClient) {
	        return ;
	    }
	    
		switch (state) {
		case 1:
		case 2:
			return;
		}
		state = 1;
		EventExecutor loop = ctx.executor();
		lastReadTime = System.nanoTime();
		if (readerIdleTimeNanos > 0) {
			readerIdleTimeout = loop.schedule(new ReaderIdleTimeoutTask(ctx), readerIdleTimeNanos, TimeUnit.NANOSECONDS);
		}
	}

	private void destroy() {
		state = 2;

		if (readerIdleTimeout != null) {
			readerIdleTimeout.cancel(false);
			readerIdleTimeout = null;
		}
	}
	
	/**
	 * Is called when an {@link IdleStateEvent} should be fired. This
	 * implementation calls
	 * {@link ChannelHandlerContext#fireUserEventTriggered(Object)}.
	 */
	protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
		ctx.fireUserEventTriggered(evt);
	}

	private final class ReaderIdleTimeoutTask implements Runnable {

		private final ChannelHandlerContext ctx;

		ReaderIdleTimeoutTask(ChannelHandlerContext ctx) {
			this.ctx = ctx;
		}

		@Override
		public void run() {
			if (!ctx.channel().isOpen()) {
				return;
			}
			
			// idle timeout
			if (isReaderIdel.get()) {
				long currentTime = System.nanoTime();
				long lastReadTime = KeepAliveHandler.this.lastReadTime;
				Long lastWriteTime = HeartbeatTimestamp.getHeartbeatWriteTimestamp(ctx);
				if (lastWriteTime != null && lastWriteTime > lastReadTime) {
				    lastReadTime = lastWriteTime;
				}
				long nextDelay = readerIdleTimeKeepAliveTimeOutNanos - (currentTime - lastReadTime);
				
				LOG.info("readerIdleTimeKeepAliveTimeOutNanos: " + readerIdleTimeKeepAliveTimeOutNanos);
				LOG.info("(currentTime - lastReadTime) : " + (currentTime - lastReadTime));
				
				if (nextDelay <= 0) {
					LOG.warn("====>keepAlive timeout will close..." + ctx.channel().remoteAddress());
					ctx.fireExceptionCaught(new KeepAliveException());

				} else {
					isReaderIdel.compareAndSet(true, false);
					KeepAliveHandler.this.lastReadTime = System.nanoTime();
					readerIdleTimeout = ctx.executor().schedule(this, readerIdleTimeNanos, TimeUnit.NANOSECONDS);
				}
			} else {
				long currentTime = System.nanoTime();
				long lastReadTime = KeepAliveHandler.this.lastReadTime;
				Long lastWriteTime = HeartbeatTimestamp.getHeartbeatWriteTimestamp(ctx);
                if (lastWriteTime != null && lastWriteTime > lastReadTime) {
                    lastReadTime = lastWriteTime;
                }
				long nextDelay = readerIdleTimeNanos - (currentTime - lastReadTime);
				if (nextDelay <= 0) {
					// time out
					isReaderIdel.compareAndSet(false, true);
					//if (readerIdleTimeKeepAliveTimeOutNanos > 0) {
					if (readerIdleTimeNanos > 0) {
						//readerIdleTimeout = ctx.executor().schedule(this, readerIdleTimeKeepAliveTimeOutNanos, TimeUnit.NANOSECONDS);
					    readerIdleTimeout = ctx.executor().schedule(this, readerIdleTimeNanos, TimeUnit.NANOSECONDS);
						Command c = new Command();
						c.setAction(CommandConstants.KEEPALIVE);
						c.putHeadValue(CommandConstants.ALIVE_MSG, CommandConstants.ALIVE_MSG_CONTENT);
						ctx.channel().writeAndFlush(c);
						
						LOG.info("==============> send heartbeat msg.");
					} else {
						// close....
						LOG.warn("====> readerIdleTimeKeepAliveTimeOutNanos set is 0  keepAlive timeout will close..." + ctx.channel().remoteAddress());
						ctx.fireExceptionCaught(new KeepAliveException());
					}
				} else {
					// Read occurred before the timeout - set a new timeout with
					// shorter delay.
				    if (nextDelay > nextDelayLimit) {
				        nextDelay = readerIdleTimeNanos;
				    }
					readerIdleTimeout = ctx.executor().schedule(this, nextDelay, TimeUnit.NANOSECONDS);
				}
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
	}
	
}
