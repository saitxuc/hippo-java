package com.hippo.network.transport;

import java.io.IOException;

import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.network.FutureResponse;
import com.hippo.network.ResponseCallback;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.coder.CoderInitializer;

/**
 * 
 * @author saitxuc
 * @param <T>
 *
 */
public class TransportFilter<T> extends LifeCycleSupport implements Transport, TransportListener<T>{
	
	protected Transport next;
	
	protected TransportListener transportListener;
	
	public TransportFilter(Transport next) {
		this.next = next;
	}
	
	@Override
	public void oneway(Object command) throws IOException {
		if (next == null) {
            throw new IOException("The next channel has not been set.");
        }
		next.oneway(command);
	}

	@Override
	public FutureResponse asyncRequest(Object command,
			ResponseCallback responseCallback) throws IOException {
		if (next == null) {
            throw new IOException("The next channel has not been set.");
        }
		return next.asyncRequest(command, responseCallback);
	}

	@Override
	public Object request(Object command) throws IOException {
		if (next == null) {
            throw new IOException("The next channel has not been set.");
        }
		return next.request(command);
	}

	@Override
	public Object request(Object command, long timeout) throws IOException {
		if (next == null) {
            throw new IOException("The next channel has not been set.");
        }
		return next.request(command, timeout);
	}

	@Override
	public String getRemoteAddress() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		return next.getRemoteAddress();
	}

	@Override
	public boolean isDisposed() {
		return next.isDisposed();
	}

	@Override
	public boolean isConnected() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		return next.isConnected();
	}

	@Override
	public boolean isReconnectSupported() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		return next.isReconnectSupported();
	}

	@Override
	public void handleCommand(Object ctx, Command command)
			throws HippoException {
		transportListener.handleCommand(ctx, command);
	}

	

	@Override
	public boolean isStarted() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		return next.isStarted();
	}

	public TransportListener getTransportListener() {
		return transportListener;
	}

	public void setTransportListener(TransportListener channelListener) {
		this.transportListener = channelListener;
        if (channelListener == null) {
            next.setTransportListener(null);
        } else {
            next.setTransportListener(this);
        }
		
	}

	@Override
	public void doInit() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.init();
	}

	@Override
	public void doStart() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.start();
	}

	@Override
	public void doStop() {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.stop();
	}

	@Override
	public void handleException(Object ctx) throws HippoException {
		transportListener.handleException(ctx);
	}

	@Override
	public void onCommand(Object ctx, Command command) throws HippoException {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.onCommand(ctx, command);
	}

	@Override
	public void onChannelException(Object ctx) throws HippoException {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.onChannelException(ctx);
	}

	@Override
	public void setCoderInitializer(CoderInitializer coderInitializer) {
		if (next == null) {
            throw new IllegalStateException("The next channel has not been set.");
        }
		next.setCoderInitializer(coderInitializer);
	}

	@Override
	public void handleEvent(T eventtype) throws HippoException {
		transportListener.handleEvent(eventtype);
	}
	

}
