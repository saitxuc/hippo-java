package com.hippo.network.transport.nio.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.listener.TransportEventEnum;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandManager;

/**
 * 
 * @author saitxuc
 *
 */
public class RecoveryNioTransport extends NioTransport {

    private static final int DEFAULT_INITIAL_RECONNECT_DELAY = 10;
    private long initialReconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private long maxReconnectDelay = 1000 * 30;
    private long reconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private double backOffMultiplier = 2d;

    private final Object sleepMutex = new Object();
    private final Object connecting = new Object();
    private AtomicBoolean reconnecting = new AtomicBoolean(false);

    private ExecutorService executor = null;
    
    private Object m = new Object(); 

    public RecoveryNioTransport(String host, int port) {
        super(host, port);
    }

    public RecoveryNioTransport(String host, int port, CommandManager commandManager) {
        super(host, port, commandManager);
    }

    @Override
    public boolean isReconnectSupported() {
        return true;
    }

    private void reconnect() {
        if (reconnecting.compareAndSet(false, true)) {
            this.doReconnect();
            synchronized (connecting) {
                try {
                    connecting.notifyAll();
                } catch (Exception e) {

                }
            }
            reconnecting.compareAndSet(true, false);
        }
    }

    private void doReconnect() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" this hippo client is reconnecting. ");
        }

        if (channel != null && !channel.isActive()) {
            try {
                channel.closeFuture().sync();
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        while (true) {
            try {
                if (this.closed || this.stoped.get()) {
                    break;
                }
                channel = boot.connect(host, port).sync().channel();
                if (channel.isActive()) {
                    this.transportListener.handleEvent(TransportEventEnum.EVENT_RECONNECT);
                    break;
                }
            } catch (Exception e) {
                try {
                    if (channel != null && !channel.isActive()) {
                        channel.closeFuture().sync();
                    }
                } catch (Exception e1) {
                    LOG.error(e1.getMessage(), e1);
                }
                //transport = null;
            }
            LOG.info("hippo client tcp transport reconnect happen error.  Sleeping for " + reconnectDelay + " milli(s) waiting...");
            doDelay();
            if (reconnectDelay == maxReconnectDelay) {
                resetReconnectDelay();
            }
        }
    }

    private void doDelay() {
        if (reconnectDelay > 0) {
            synchronized (sleepMutex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Waiting " + reconnectDelay + " ms before attempting connection");
                }
                try {
                    sleepMutex.wait(reconnectDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            reconnectDelay *= backOffMultiplier;
            if (reconnectDelay > maxReconnectDelay) {
                reconnectDelay = maxReconnectDelay;
            }
        }
    }

    private void resetReconnectDelay() {
        reconnectDelay = initialReconnectDelay;
    }

    @Override
    public void doInit() {
        executor = Executors.newSingleThreadExecutor();
        super.doInit();
    }

    @Override
    public void doStart() {
        try {
            // Start the client.
            channel = boot.connect(host, port).sync().channel();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            while (true) {
                if (channel != null && channel.isActive()) {
                    break;
                }
                synchronized (connecting) {
                    try {
                        connecting.wait(1000);
                    } catch (InterruptedException e1) {

                    }
                }
            }

        }
    }

    public void doStop() {
        this.closed = true;
        try {
            synchronized (sleepMutex) {
                sleepMutex.notifyAll();
            }
        } catch (Exception e) {
        } finally {
            if (executor != null) {
                executor.shutdownNow();
                executor = null;
            }
        }
        super.doStop();
    }

    public void close() {
        stop();
    }

    @Override
    public void onChannelException(Object ctx) throws HippoException {
        if (this.isReconnectSupported()) {
            if (!this.closed) {
                this.reconnect();
            }
        }
        super.onChannelException(ctx);
    }

}
