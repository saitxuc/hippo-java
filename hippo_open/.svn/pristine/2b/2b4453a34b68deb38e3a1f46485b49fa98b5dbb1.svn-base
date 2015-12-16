package com.pinganfu.hippo.network.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.IdGenerator;
import com.pinganfu.hippo.common.exception.ConfigurationException;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.common.util.IntrospectionSupport;
import com.pinganfu.hippo.common.util.URISupport;
import com.pinganfu.hippo.common.util.URISupport.CompositeData;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.exception.TransportException;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportFactoryFinder;
import com.pinganfu.hippo.network.transport.TransportListener;
import com.pinganfu.hippo.network.transport.nio.NettyTransportFactory;
import com.pinganfu.hippo.network.transport.nio.coder.CoderInitializer;
import com.pinganfu.hippo.network.transport.nio.coder.DefaultCoderInitializer;

/**
 * @author saitxuc
 * write 2014-7-15
 */
public class TransportConnectionFactory implements ConnectionFactory {

    protected static final Logger LOG = LoggerFactory.getLogger(TransportConnectionFactory.class);

    //public static final String DEFAULT_BROKER_URL = "tcp://localhost:61300";
    public static final String DEFAULT_NIO_TYPE = "netty";

    private String nioType = DEFAULT_NIO_TYPE;

    private IdGenerator clientIdGenerator;
    private IdGenerator connectionIdGenerator;
    private String connectionIDPrefix;
    private String clientIDPrefix;
    protected URI brokerURL;
    protected String userName;
    protected String password;
    protected String clientID;
    private CoderInitializer coderInitializer;
    protected CommandManager commandManager;

    protected static final Object lock = new Object();

    public TransportConnectionFactory(String brokerURL) {
        this(createURI(brokerURL));
    }

    public TransportConnectionFactory(URI brokerURL) {
        setBrokerURL(brokerURL.toString());
    }

    public TransportConnectionFactory copy() {
        try {
            return (TransportConnectionFactory) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("This should never happen: " + e, e);
        }
    }

    public TransportConnectionFactory(String userName, String password, URI brokerURL) {
        setUserName(userName);
        setPassword(password);
        setBrokerURL(brokerURL.toString());
    }

    public TransportConnectionFactory(String userName, String password, String brokerURL) {
        setUserName(userName);
        setPassword(password);
        setBrokerURL(brokerURL);
    }

    @Override
    public Connection createConnection() throws HippoException {
        return createConnection(userName, password);
    }

    @Override
    public Connection createConnection(String userName, String password) throws HippoException {
        if (brokerURL == null) {
            throw new ConfigurationException("brokerURL not set.");
        }
        TransportConnection connection = null;
        try {
            String schema = createShema(brokerURL.getScheme());

            Transport transport = createTransport(schema);
            connection = createHippoConnection(transport);
            transport.setTransportListener(connection);
            if (coderInitializer != null) {
                transport.setCoderInitializer(coderInitializer);
            }
            //connection.setUserName(userName);
            //connection.setPassword(password);
            transport.start();

            if (clientID != null)
                connection.setDefaultClientID(clientID);

            return connection;
        } catch (HippoException e) {
            // Clean up!
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
            throw e;
        } catch (Exception e) {
            // Clean up!
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
            LOG.error("Could not connect to broker URL: " + brokerURL , e);
            HippoException ex = new HippoException("Could not connect to broker URL: " + brokerURL + ". Reason: " + e.getMessage());
            ex.setLinkedException(e);
            throw ex;
        }
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = createURI(brokerURL);

        if (this.brokerURL.getQuery() != null) {
            try {

                Map map = URISupport.parseQuery(this.brokerURL.getQuery());
                if (buildFromMap(IntrospectionSupport.extractProperties(map, "hippo."))) {
                    this.brokerURL = URISupport.createRemainingURI(this.brokerURL, map);
                }

            } catch (URISyntaxException e) {
            }

        } else {
            try {
                CompositeData data = URISupport.parseComposite(this.brokerURL);
                if (buildFromMap(IntrospectionSupport.extractProperties(data.getParameters(), "hippo."))) {
                    this.brokerURL = data.toURI();
                }
            } catch (URISyntaxException e) {
            }
        }
    }

    public Transport createTransport(String schema) {
        synchronized (lock) {
            TransportFactory transportFactory = TransportFactoryFinder.getTransportFactory(schema);
            String host = brokerURL.getHost();
            int port = brokerURL.getPort();
            try {
                final Transport transport = transportFactory.connect(host, port, commandManager);
                return transport;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return null;
            }
        }

    }

    private TransportConnection createHippoConnection(Transport transport) throws Exception {
        TransportConnection connection = new TransportConnection(transport, getClientIdGenerator(), getConnectionIdGenerator());
        return connection;
    }

    public boolean buildFromMap(Map properties) {
        boolean rc = false;
        rc |= IntrospectionSupport.setProperties(this, properties);
        return rc;
    }

    private static URI createURI(String brokerURL) {
        try {
            return new URI(brokerURL);
        } catch (URISyntaxException e) {
            throw (IllegalArgumentException) new IllegalArgumentException("Invalid broker URI: " + brokerURL).initCause(e);
        }
    }

    protected synchronized IdGenerator getClientIdGenerator() {
        if (clientIdGenerator == null) {
            if (clientIDPrefix != null) {
                clientIdGenerator = new IdGenerator(clientIDPrefix);
            } else {
                clientIdGenerator = new IdGenerator();
            }
        }
        return clientIdGenerator;
    }

    protected void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    protected synchronized IdGenerator getConnectionIdGenerator() {
        if (connectionIdGenerator == null) {
            if (connectionIDPrefix != null) {
                connectionIdGenerator = new IdGenerator(connectionIDPrefix);
            } else {
                connectionIdGenerator = new IdGenerator();
            }
        }
        return connectionIdGenerator;
    }

    protected void setConnectionIdGenerator(IdGenerator connectionIdGenerator) {
        this.connectionIdGenerator = connectionIdGenerator;
    }

    public URI getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(URI brokerURL) {
        this.brokerURL = brokerURL;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getConnectionIDPrefix() {
        return connectionIDPrefix;
    }

    public void setConnectionIDPrefix(String connectionIDPrefix) {
        this.connectionIDPrefix = connectionIDPrefix;
    }

    public CommandManager getCommandManager() {
        return commandManager;
    }

    public void setCommandManager(CommandManager commandManager) {
        this.commandManager = commandManager;
    }

    @Override
    public void setNioType(String ntype) {
        this.nioType = ntype;
    }

    protected String createShema(String scheme) {
        return (nioType + "-" + scheme);
    }


    public void setCoderInitializer(CoderInitializer coderInitializer) {
        this.coderInitializer = coderInitializer;
    }

}
