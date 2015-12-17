package com.hippo.network.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.LongSequenceGenerator;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.network.Session;
import com.hippo.network.SessionId;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.Response;
import com.hippo.network.command.SessionInfo;

/**
 * @author saitxuc
 * write 2014-7-10
 */
public class TransportSession extends LifeCycleSupport implements Session {
	
	protected final static Logger LOG = LoggerFactory.getLogger(TransportSession.class);
	
	private Object ensureSessionInfoSentMutex =  new Object();
    private boolean isSessionInfoSentToBroker = false;
	private volatile boolean closed = false;
	private TransportConnection connection;
	private SessionInfo info;
	
	protected final LongSequenceGenerator providrIdGenerator = new LongSequenceGenerator();
	
	protected TransportSession(TransportConnection connection, SessionId sessionId) throws HippoException {
        this.connection = connection;
        this.info = new SessionInfo(connection.getConnectionInfo(), sessionId.getValue());
        connection.addSession(this);
        if (connection.isStarted()) {
            start();
        }
	}
	
	@Override
	public void close() throws HippoException {
		boolean interrupted = Thread.interrupted();
        if(connection.getTransportChannel().isConnected()){
        	Command command = info.createRemoveCommand();
            connection.asyncSendPacket(command);
        }
        dispose();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
	}
	
	public synchronized void dispose() throws HippoException {
		if(!closed) {
			connection.removeSession(this);
	        //this.transactionContext = null;
	        closed = true;
		}
	}
	
    protected void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The cache provider is closed");
        }
    }	
    
	@Override
	public void doInit() {
		
	}

	@Override
	public void doStart() {
		checkClosed();
		try {
			ensureSessionInfoSent();
		} catch (HippoException e) {
			LOG.error(e.getMessage(), e);
			stop();
		}

	}
	
	@Override
	public void doStop() {
		try {
			close();
		} catch (HippoException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public Response send(Command data, long timeout) throws HippoException {
		checkClosed();
		ensureSessionInfoSent();
		Response response = (Response)connection.syncSendPacket(data, timeout);
		if(response.isFailure()) {
			LOG.error(" command action : " + data.getAction() + " response is failure. ");
		}
		return response;
	}

	@Override
	public void asnysend(Command data) throws HippoException {
		checkClosed();
		ensureSessionInfoSent();
		connection.asyncSendPacket(data);
	}

	@Override
	public void reset() {
		this.isSessionInfoSentToBroker = false;
	}

	protected void ensureSessionInfoSent() throws HippoException  {
		synchronized(this.ensureSessionInfoSentMutex) {
			if (isSessionInfoSentToBroker || closed) {
                return;
            }
			Response rsp = null;
			try {
				rsp = (Response)this.connection.syncSendPacket(this.info.copy(), CommandConstants.REQUEST_TIMEOUT);
			} catch (HippoException e) {
				LOG.error(e.getMessage(), e);
				throw new HippoException(" session info request to broker happen error. ");
			}
			if (rsp.isFailure()) {
				throw new HippoException(
						" session info response from broker happen error. ");
			}
	        this.isSessionInfoSentToBroker = true;
		}
	}
	
}


