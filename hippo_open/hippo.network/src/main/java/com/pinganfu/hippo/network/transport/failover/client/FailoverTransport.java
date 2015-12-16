package com.pinganfu.hippo.network.transport.failover.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.util.ExcutorUtils;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.impl.TransportConnection;
import com.pinganfu.hippo.network.transport.CompositeTransport;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportFactoryFinder;
import com.pinganfu.hippo.network.transport.TransportListener;
import com.pinganfu.hippo.network.transport.nio.client.NioTransport;

public class FailoverTransport extends NioTransport implements CompositeTransport {

    private ExecutorService reconnectExecutor = null;
    private boolean started;
    private boolean randomize = false;

    private final Object reconnectMutex = new Object();
    private final Object sleepMutex = new Object();

    private boolean doRebalance = false;

    private boolean firstConnection = true;

    private final CopyOnWriteArrayList<URI> uris = new CopyOnWriteArrayList<URI>();
    private final CopyOnWriteArrayList<URI> updated = new CopyOnWriteArrayList<URI>();
    private final AtomicReference<Transport> connectedTransport = new AtomicReference<Transport>();
    private URI connectedTransportURI;
    private URI failedConnectTransportURI;

    private int connectFailures;
    private boolean disposed;
    private boolean connected;

    private boolean updateURIsSupported = true;
    private String updateURIsURL = null;
    private boolean rebalanceUpdateURIs = true;

    public FailoverTransport() {
        this(null, null);
    }

    public FailoverTransport(URI[] uris) {
        this(uris, null);
    }

    public FailoverTransport(CommandManager commandManager) {
        super(null, 0);
    }

    public FailoverTransport(URI[] uris, CommandManager commandManager) {
        super(null, 0, commandManager);

        if (uris != null) {
            add(false, uris);
        }
    }

    @Override
    public void doInit() {
        // Setup a task that is used to reconnect the a connection async.
        reconnectExecutor = ExcutorUtils.startSingleExcutor("Failover Worker");

//        super.doInit();
    }

    @Override
    public void doStart() {
        synchronized (reconnectMutex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Started " + this);
            }
            if (started) {
                return;
            }
            started = true;
            reconnect(false);
        }

        while (true) {
            if (connected) {
                break;
            }
            synchronized (connecting) {
                try {
                    connecting.wait(1000);
                } catch (InterruptedException e1) {}
            }
            
            reconnect(false);
        }
    }
    
    final boolean doReconnect() {
        synchronized (reconnectMutex) {

            // First ensure we are up to date.
            doUpdateURIsFromDisk();

            List<URI> connectList = getConnectList();
            if (!connectList.isEmpty()) {
                URI uri = null;
                Transport transport = null;
                Iterator<URI> iter = connectList.iterator();
                while ((transport != null || iter.hasNext()) && (connectedTransport.get() == null && !disposed)) {
                    try {
                        // We could be starting with a backup and if so we wait to grab a
                        // URI from the pool until next time around.
                        if (transport == null) {
                            uri = iter.next();
                            transport = createTransport(uri);
                        }

                        if (LOG.isInfoEnabled()) {
                            LOG.info("Attempting  " + connectFailures + "th  connect to: " + uri);
                        }
                        TransportListener transportListener = super.getTransportListener();
                        transport.setTransportListener(transportListener);
                        ((TransportConnection) transportListener).setTransport(transport);

                        transport.start();

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Connection established");
                        }
                        connectedTransport.set(transport);
                        reconnectMutex.notifyAll();
                        connectFailures = 0;

                        if (firstConnection) {
                            firstConnection = false;
                            LOG.info("Successfully connected to " + uri);
                        } else {
                            LOG.info("Successfully reconnected to " + uri);
                        }

                        connected = true;
                        return false;
                    } catch (Exception e) {
                        if (LOG.isInfoEnabled()) {
                            LOG.warn("Connect fail to: " + uri + ", reason: " + e);
                            e.printStackTrace();
                        }
                        if (transport != null) {
                            try {
                                transport.stop();
                                transport = null;
                            } catch (Exception ee) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Stop of failed transport: " + transport + " failed with reason: " + ee);
                                }
                            }
                        }
                    }
                }
            }
        }

        return !disposed;
    }

    private List<URI> getConnectList() {
        ArrayList<URI> l = new ArrayList<URI>(uris);
        boolean removed = false;
        if (failedConnectTransportURI != null) {
            removed = l.remove(failedConnectTransportURI);
        }
        if (randomize) {
            // Randomly, reorder the list by random swapping
            for (int i = 0; i < l.size(); i++) {
                int p = (int) (Math.random() * 100 % l.size());
                URI t = l.get(p);
                l.set(p, l.get(i));
                l.set(i, t);
            }
        }
        if (removed) {
            l.add(failedConnectTransportURI);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("urlList connectionList:" + l + ", from: " + uris);
        }
        return l;
    }

    private Transport createTransport(URI uri) {
        TransportFactory transportFactory = TransportFactoryFinder.getTransportFactory("netty-" + uri.getScheme());
        try {
            final Transport transport = transportFactory.connect(uri, null);
            return transport;
        } catch (Exception e) {
            LOG.error("Create transport error", e);
            return null;
        }
    }

    public void reconnect(boolean rebalance) {
        synchronized (reconnectMutex) {
            if (started) {
                if (rebalance) {
                    doRebalance = true;
                }
                LOG.debug("Waking up reconnect task");
                reconnectExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                		if (connected) {
                            return;
                        }
                        doReconnect();
                    }
                });
            } else {
                LOG.debug("Reconnect was triggered but transport is not started yet. Wait for start to connect the transport.");
            }
        }
    }

    @Override
    public void doStop() {
        try {
            synchronized (reconnectMutex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stopped " + this);
                }
                if (!started) {
                    return;
                }
                started = false;

                reconnectMutex.notifyAll();
            }
            synchronized (sleepMutex) {
                sleepMutex.notifyAll();
            }
        } finally {
            ExcutorUtils.shutdown(reconnectExecutor);
        }

        super.doStop();
    }

    @Override
    public void add(boolean rebalance, URI u[]) {
        boolean newURI = false;
        for (URI uri : u) {
            if (!contains(uri)) {
                uris.add(uri);
                newURI = true;
            }
        }
        if (newURI) {
            reconnect(rebalance);
        }
    }

    @Override
    public void remove(boolean rebalance, URI u[]) {
        for (URI uri : u) {
            uris.remove(uri);
        }
        // rebalance is automatic if any connected to removed/stopped broker
    }

    private boolean contains(URI newURI) {
        boolean result = false;
        for (URI uri : uris) {
            if (newURI.getPort() == uri.getPort()) {
                InetAddress newAddr = null;
                InetAddress addr = null;
                try {
                    newAddr = InetAddress.getByName(newURI.getHost());
                    addr = InetAddress.getByName(uri.getHost());
                } catch (IOException e) {

                    if (newAddr == null) {
                        LOG.error("Failed to Lookup INetAddress for URI[ " + newURI + " ] : " + e);
                    } else {
                        LOG.error("Failed to Lookup INetAddress for URI[ " + uri + " ] : " + e);
                    }

                    if (newURI.getHost().equalsIgnoreCase(uri.getHost())) {
                        result = true;
                        break;
                    } else {
                        continue;
                    }
                }

                if (addr.equals(newAddr)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public void onChannelException(Object ctx) throws HippoException {
        reconnect(false);
        super.onChannelException(ctx);
    }

    private void doUpdateURIsFromDisk() {
        // If updateURIsURL is specified, read the file and add any new
        // transport URI's to this FailOverTransport.
        // Note: Could track file timestamp to avoid unnecessary reading.
        String fileURL = getUpdateURIsURL();
        if (fileURL != null) {
            BufferedReader in = null;
            String newUris = null;
            StringBuffer buffer = new StringBuffer();

            try {
                in = new BufferedReader(getURLStream(fileURL));
                while (true) {
                    String line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    buffer.append(line);
                }
                newUris = buffer.toString();
            } catch (IOException ioe) {
                LOG.error("Failed to read updateURIsURL: " + fileURL, ioe);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ioe) {
                        // ignore
                    }
                }
            }

            processNewTransports(isRebalanceUpdateURIs(), newUris);
        }
    }

    private final void processNewTransports(boolean rebalance, String newTransports) {
        if (newTransports != null) {
            newTransports = newTransports.trim();
            if (newTransports.length() > 0 && isUpdateURIsSupported()) {
                List<URI> list = new ArrayList<URI>();
                StringTokenizer tokenizer = new StringTokenizer(newTransports, ",");
                while (tokenizer.hasMoreTokens()) {
                    String str = tokenizer.nextToken();
                    try {
                        URI uri = new URI(str);
                        list.add(uri);
                    } catch (Exception e) {
                        LOG.error("Failed to parse broker address: " + str, e);
                    }
                }
                if (list.isEmpty() == false) {
                    try {
                        updateURIs(rebalance, list.toArray(new URI[list.size()]));
                    } catch (IOException e) {
                        LOG.error("Failed to update transport URI's from: " + newTransports, e);
                    }
                }
            }
        }
    }

    public void updateURIs(boolean rebalance, URI[] updatedURIs) throws IOException {
        if (isUpdateURIsSupported()) {
            HashSet<URI> copy = new HashSet<URI>(this.updated);
            updated.clear();
            if (updatedURIs != null && updatedURIs.length > 0) {
                for (URI uri : updatedURIs) {
                    if (uri != null && !updated.contains(uri)) {
                        updated.add(uri);
                    }
                }
                if (!(copy.isEmpty() && updated.isEmpty()) && !copy.equals(new HashSet<URI>(updated))) {
                    /// buildBackups();
                    synchronized (reconnectMutex) {
                        reconnect(rebalance);
                    }
                }
            }
        }
    }

    private InputStreamReader getURLStream(String path) throws IOException {
        InputStreamReader result = null;
        URL url = null;
        try {
            url = new URL(path);
            result = new InputStreamReader(url.openStream());
        } catch (MalformedURLException e) {
            // ignore - it could be a path to a a local file
        }
        if (result == null) {
            result = new FileReader(path);
        }
        return result;
    }

    @Override
    public String toString() {
        return connectedTransportURI == null ? "unconnected" : connectedTransportURI.toString();
    }

    public String getUpdateURIsURL() {
        return updateURIsURL;
    }

    public void setUpdateURIsURL(String updateURIsURL) {
        this.updateURIsURL = updateURIsURL;
    }

    public boolean isRebalanceUpdateURIs() {
        return rebalanceUpdateURIs;
    }

    public void setRebalanceUpdateURIs(boolean rebalanceUpdateURIs) {
        this.rebalanceUpdateURIs = rebalanceUpdateURIs;
    }

    public boolean isUpdateURIsSupported() {
        return updateURIsSupported;
    }

    public void setUpdateURIsSupported(boolean updateURIsSupported) {
        this.updateURIsSupported = updateURIsSupported;
    }

    public URI getConnectedTransportURI() {
        return connectedTransportURI;
    }

    public void setConnectedTransportURI(URI connectedTransportURI) {
        this.connectedTransportURI = connectedTransportURI;
    }
}
