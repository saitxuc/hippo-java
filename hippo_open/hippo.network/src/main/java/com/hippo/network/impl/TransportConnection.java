package com.hippo.network.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.IdGenerator;
import com.hippo.common.LongSequenceGenerator;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.common.listener.EmptyBaseEventListener;
import com.hippo.common.listener.EventListener;
import com.hippo.common.listener.ExceptionListener;
import com.hippo.common.listener.NettyBaseEventListener;
import com.hippo.common.listener.TransportEventEnum;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.common.util.NetUtils;
import com.hippo.network.Connection;
import com.hippo.network.ConnectionId;
import com.hippo.network.Session;
import com.hippo.network.SessionId;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.RemoveConnectionCommand;
import com.hippo.network.command.Response;
import com.hippo.network.exception.ConnectionClosedException;
import com.hippo.network.exception.ConnectionFailedException;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportListener;
import com.hippo.network.transport.nio.NettyConnection;

/**
 * @author saitxuc
 * write 2014-7-7
 */
public class TransportConnection extends LifeCycleSupport implements Connection, TransportListener<TransportEventEnum> {

    protected static final Logger LOG = LoggerFactory.getLogger(TransportConnection.class);

    private static final int SESSION_MAX_NUM = 50;

    private Transport transport;

    private IdGenerator clientIdGenerator;

    private IdGenerator connectionIdGenerator;

    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();

    private final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<Session>();

    private ConnectionInfo info;
    private SessionId connectionSessionId;
    private List<ExceptionListener> exceptionListeners = new ArrayList<ExceptionListener>();
    private List<EventListener<TransportEventEnum>> eventListeners = new ArrayList<EventListener<TransportEventEnum>>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    //private final AtomicBoolean transportFailed = new AtomicBoolean(false);

    private Object ensureConnectionInfoSentMutex = new Object();
    private boolean isConnectionInfoSentToBroker = false;
    private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private int sendTimeout;

    private IOException firstFailureError;

    private int sessionMaxNum = SESSION_MAX_NUM;

    private ExecutorService stopTaskExcutor;

    public TransportConnection(final Transport transport, IdGenerator clientIdGenerator, IdGenerator connectionIdGenerator) throws IOException {
        this.transport = transport;
        this.clientIdGenerator = clientIdGenerator;
        String uniqueId = connectionIdGenerator.generateId();
        this.info = new ConnectionInfo(new ConnectionId(uniqueId));
        this.connectionSessionId = new SessionId(info.getConnectionId(), -1);
    }

    public boolean isStarted() {
        return this.started.get();
    }

    public boolean isStopped() {
        return this.stoped.get();
    }

    @Override
    public Session createSession() throws HippoException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        if (sessions.size() > sessionMaxNum) {
            throw new HippoException("created session number of this connection upper the limit.");
        }
        return new TransportSession(this, getNextSessionId());
    }

    @Override
    public String getClientID() throws HippoException {
        checkClosedOrFailed();
        return this.info.getClientId();
    }

    @Override
    public void setClientID(String clientID) throws HippoException {
        checkClosedOrFailed();
        this.info.setClientId(clientID);

    }

    public void setDefaultClientID(String clientID) throws HippoException {
        this.info.setClientId(clientID);
        //this.userSpecifiedClientID = true;
    }

    /**
     */
    public Transport getTransportChannel() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    protected synchronized void checkClosedOrFailed() throws HippoException {
        checkClosed();
        if (!this.transport.isConnected()) {
            throw new ConnectionFailedException(firstFailureError);
        }
    }

    protected void transportFailed(IOException error) {
        //transportFailed.set(true);
        if (firstFailureError == null) {
            firstFailureError = error;
        }
    }

    public IOException getFirstFailureError() {
        return firstFailureError;
    }

    protected synchronized void checkClosed() throws HippoException {
        if (closed.get()) {
            throw new ConnectionClosedException();
        }
    }

    /**
     * @return sessionId
     */
    protected SessionId getNextSessionId() {
        return new SessionId(info.getConnectionId(), sessionIdGenerator.getNextSequenceId());
    }

    /**
     * @return ConnectionInfo
     */
    public ConnectionInfo getConnectionInfo() {
        return this.info;
    }

    @Override
    public void close() throws HippoException {
        if (closed.compareAndSet(false, true)) {
            if (this.transport.isConnected()) {
                for (Iterator<Session> i = sessions.iterator(); i.hasNext();) {
                    Session session = i.next();
                    session.stop();
                }
                if (transport.isConnected()) {
                    RemoveConnectionCommand removeConnectionCommand = new RemoveConnectionCommand();
                    removeConnectionCommand.setConnectionId(this.getConnectionInfo().getConnectionId());
                    try {
                        transport.oneway(removeConnectionCommand);
                    } catch (IOException e) {
                        LOG.error("Transport connect close happen error. ", e);
                    }
                }
            }
            this.transport.stop();
        }
    }

    @Override
    public void doInit() {
    	instanceEventListener();
        stopTaskExcutor = ExcutorUtils.startSingleExcutor(" The Connection start single excutor handle asny work. ");
    }

    @Override
    public void doStart() {
        try {
            this.transport.start();
            checkClosedOrFailed();
            ensureConnectionInfoSent();
        } catch (Exception e) {
            this.transport.stop();
            throw new RuntimeException(" Connection dostart happen error. ", e);
        }
        for (Iterator<Session> i = sessions.iterator(); i.hasNext();) {
            Session session = i.next();
            session.start();
        }
    }

    @Override
    public void doStop() {
        try {
            this.close();
            if (stopTaskExcutor != null) {
                stopTaskExcutor.shutdown();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    protected void ensureConnectionInfoSent() throws HippoException {
        synchronized (this.ensureConnectionInfoSentMutex) {
            // Can we skip sending the ConnectionInfo packet??
            if (isConnectionInfoSentToBroker || closed.get()) {
                return;
            }
            //shouldn't this check be on userSpecifiedClientID rather than the value of clientID?
            if (info.getClientId() == null || info.getClientId().trim().length() == 0) {
                info.setClientId(clientIdGenerator.generateId());
            }
            if (info.getClientIp() == null || info.getClientIp().trim().length() == 0) {
                info.setClientIp(NetUtils.getLocalHost());
            }

            Response rsp = null;
            try {
                rsp = (Response) transport.request(info.copy(), CommandConstants.REQUEST_TIMEOUT);
            } catch (IOException e) {
                e.printStackTrace();
                throw new HippoException(" connection info request to broker happen error. ", HippoCodeDefine.HIPPO_RECONNECTION_ERROR);
            }
            if (rsp.isFailure()) {
                throw new HippoException(" connection info response from broker happen error. ", HippoCodeDefine.HIPPO_RECONNECTION_ERROR);
            }
            this.isConnectionInfoSentToBroker = true;
        }
    }

    public void asyncSendPacket(Command command) throws HippoException {
        checkClosedOrFailed();
        doAsyncSendPacket(command);
    }

    private void doAsyncSendPacket(Command command) throws HippoException {
        try {
            ensureConnectionInfoSent();
            this.transport.oneway(command);
        } catch (IOException e) {
            throw new HippoException(e.getMessage());
        }
    }

    @Override
    public Object syncSendPacket(Command command, long timeout) throws HippoException {
        checkClosedOrFailed();
        return doSnycSendPacket(command, timeout);
    }

    private Object doSnycSendPacket(Command command, long timeout) throws HippoException {
        try {
            ensureConnectionInfoSent();
            return this.transport.request(command, timeout);
        } catch (IOException e) {
            throw new HippoException(e.getMessage());
        }
    }

    protected void addSession(Session session) throws HippoException {
        this.sessions.add(session);
    }

    protected void removeSession(Session session) {
        this.sessions.remove(session);
        //this.removeDispatcher(session);
    }

    public int getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(int sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public int getSessionMaxNum() {
        return sessionMaxNum;
    }

    public void setSessionMaxNum(int sessionMaxNum) {
        this.sessionMaxNum = sessionMaxNum;
    }

    @Override
    public void stopAsync() {
        if (this.isStopped()) {
            stopTaskExcutor.execute(new Runnable() {

                @Override
                public void run() {
                    serviceLock.writeLock().lock();
                    try {
                        doStop();
                    } catch (Throwable e) {
                        LOG.debug("Error occurred while shutting down a connection " + this, e);
                    } finally {
                        serviceLock.writeLock().unlock();
                    }
                }

            });
        }
    }

    @Override
    public void handleCommand(Object ctx, Command command) throws HippoException {
        transport.onCommand(ctx, command);
    }

    @Override
    public void handleException(Object ctx) throws HippoException {
        this.transportFailed(new IOException(" Transport happen IoException. "));
        for (Session session : sessions) {
            session.reset();
        }
        for (ExceptionListener listener : exceptionListeners) {
            listener.onException(new HippoException(" Transport happen IoException. "));
        }
        isConnectionInfoSentToBroker = false;
        transport.onChannelException(ctx);
    }

    @Override
    public void addExceptionListener(ExceptionListener listener) {
        exceptionListeners.add(listener);
    }

    @Override
    public void addEventListener(EventListener listener) {
        eventListeners.add(listener);
    }

    @Override
    public void handleEvent(TransportEventEnum eventtype) throws HippoException {

        for (EventListener<TransportEventEnum> listener : eventListeners) {
            listener.onEvent(eventtype);
        }
    }
    
    protected void instanceEventListener() {
    	this.addEventListener(new EmptyBaseEventListener());
    }
    
}
