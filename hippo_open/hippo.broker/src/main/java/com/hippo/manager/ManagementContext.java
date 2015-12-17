package com.hippo.manager;

import java.io.IOException;
import java.lang.reflect.Method;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.hippo.common.lifecycle.LifeCycleSupport;

/**
 * 
 * @author saitxuc
 * write 2014-8-12
 */
public class ManagementContext extends LifeCycleSupport {

    /**
     * Default hippo domain
     */
    public static final String DEFAULT_DOMAIN = "org.apache.hippo";

    private static final Logger LOG = LoggerFactory.getLogger(ManagementContext.class);
    private MBeanServer beanServer;
    private String jmxDomainName = DEFAULT_DOMAIN;
    private boolean useMBeanServer = true;
    private boolean createMBeanServer = true;
    private boolean locallyCreateMBeanServer;
    private boolean createConnector = true;
    private boolean findTigerMbeanServer = true;
    private String connectorHost = "localhost";
    private int connectorPort = 31099;
    private Map<String, ?> environment;
    private int rmiServerPort = 31299;
    private String connectorPath = "/jmxrmi";
    private final AtomicBoolean connectorStarting = new AtomicBoolean(false);
    private JMXConnectorServer connectorServer;
    private ObjectName namingServiceObjectName;
    private Registry registry;
    private final Map<ObjectName, ObjectName> registeredMBeanNames = new ConcurrentHashMap<ObjectName, ObjectName>();
    private boolean allowRemoteAddressInMBeanNames = true;
    private String brokerName;

    public ManagementContext() {
        this(null);
    }

    public ManagementContext(MBeanServer server) {
        this.beanServer = server;
    }

    @Override
    public void doStart(){
        
    	if(LOG.isInfoEnabled()) {
			LOG.info(" jmx mamanager context server is doing start  . ");
		}
    	
    	// lets force the MBeanServer to be created if needed
    	// fallback and use localhost
        if (connectorHost == null) {
            connectorHost = "localhost";
        }

        // force mbean server to be looked up, so we have it
        getMBeanServer();

        if (connectorServer != null) {
            try {
                if (getMBeanServer().isRegistered(namingServiceObjectName)) {
                    LOG.debug("Invoking start on mbean: {}", namingServiceObjectName);
                    getMBeanServer().invoke(namingServiceObjectName, "start", null, null);
                }
            } catch (Throwable ignore) {
                LOG.debug("Error invoking start on mbean " + namingServiceObjectName + ". This exception is ignored.", ignore);
            }

            Thread t = new Thread("JMX connector") {
                @Override
                public void run() {
                    // ensure we use MDC logging with the broker name, so people can see the logs if MDC was in use
                    if (brokerName != null) {
                        MDC.put("hippo.broker", brokerName);
                    }
                    try {
                        JMXConnectorServer server = connectorServer;
                        if (started.get() && server != null) {
                            LOG.debug("Starting JMXConnectorServer...");
                            connectorStarting.set(true);
                            try {
                                // need to remove MDC as we must not inherit MDC in child threads causing leaks
                                MDC.remove("hippo.broker");
                                server.start();
                            }finally {
                                if (brokerName != null) {
                                    MDC.put("hippo.broker", brokerName);
                                }
                                connectorStarting.set(false);
                            }
                            LOG.info("JMX consoles can connect to " + server.getAddress());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    	LOG.warn("Failed to start jmx connector: " + e.getMessage() + ". Will restart management to re-create jmx connector, trying to remedy this issue.");
                        LOG.debug("Reason for failed jmx connector start", e);
                    }  
                    finally {
                        MDC.remove("hippo.broker");
                    }
                }
            };
            t.setDaemon(true);
            t.start();
        }
        if(LOG.isInfoEnabled()) {
			LOG.info(" jmx mamanager context server finish start  . ");
		}
    }

    @Override
    public void doStop() {
    	if(LOG.isInfoEnabled()) {
			LOG.info(" jmx mamanager context server is doing doStop  . ");
		}
    	if (started.compareAndSet(true, false)) {
            MBeanServer mbeanServer = getMBeanServer();

            // unregister the mbeans we have registered
            if (mbeanServer != null) {
                for (Map.Entry<ObjectName, ObjectName> entry : registeredMBeanNames.entrySet()) {
                    ObjectName actualName = entry.getValue();
                    if (actualName != null && beanServer.isRegistered(actualName)) {
                        LOG.debug("Unregistering MBean {}", actualName);
                        try {
							mbeanServer.unregisterMBean(actualName);
						} catch (MBeanRegistrationException e) {
							LOG.error(e.getMessage());
						} catch (InstanceNotFoundException e) {
							LOG.error(e.getMessage());
						}
                    }
                }
            }
            registeredMBeanNames.clear();

            JMXConnectorServer server = connectorServer;
            connectorServer = null;
            if (server != null) {
                try {
                    if (!connectorStarting.get()) {
                        LOG.debug("Stopping jmx connector");
                        server.stop();
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to stop jmx connector: " + e.getMessage());
                }
                // stop naming service mbean
                try {
                    if (namingServiceObjectName != null && getMBeanServer().isRegistered(namingServiceObjectName)) {
                        LOG.debug("Stopping MBean {}", namingServiceObjectName);
                        getMBeanServer().invoke(namingServiceObjectName, "stop", null, null);
                        LOG.debug("Unregistering MBean {}", namingServiceObjectName);
                        getMBeanServer().unregisterMBean(namingServiceObjectName);
                    }
                } catch (Throwable ignore) {
                    LOG.warn("Error stopping and unregsitering mbean " + namingServiceObjectName + " due " + ignore.getMessage());
                }
                namingServiceObjectName = null;
            }

            if (locallyCreateMBeanServer && beanServer != null) {
                // check to see if the factory knows about this server
                List<MBeanServer> list = MBeanServerFactory.findMBeanServer(null);
                if (list != null && !list.isEmpty() && list.contains(beanServer)) {
                    LOG.debug("Releasing MBeanServer {}", beanServer);
                    MBeanServerFactory.releaseMBeanServer(beanServer);
                }
            }
            beanServer = null;
        }

        // Un-export JMX RMI registry, if it was created
        if (registry != null) {
            try {
                UnicastRemoteObject.unexportObject(registry, true);
                LOG.debug("Unexported JMX RMI Registry");
            } catch (NoSuchObjectException e) {
                LOG.debug("Error occurred while unexporting JMX RMI registry. This exception will be ignored.");
            }

            registry = null;
        }
        if(LOG.isInfoEnabled()) {
			LOG.info(" jmx mamanager context server finish doStop  . ");
		}
    }

    /**
     * Gets the broker name this context is used by, may be <tt>null</tt>
     * if the broker name was not set.
     */
    public String getBrokerName() {
        return brokerName;
    }

    /**
     * Sets the broker name this context is being used by.
     */
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /**
     * @return Returns the jmxDomainName.
     */
    public String getJmxDomainName() {
        return jmxDomainName;
    }

    /**
     * @param jmxDomainName The jmxDomainName to set.
     */
    public void setJmxDomainName(String jmxDomainName) {
        this.jmxDomainName = jmxDomainName;
    }

    /**
     * Get the MBeanServer
     *
     * @return the MBeanServer
     */
    public MBeanServer getMBeanServer() {
        if (this.beanServer == null) {
            this.beanServer = findMBeanServer();
        }
        return beanServer;
    }

    /**
     * Set the MBeanServer
     *
     * @param beanServer
     */
    public void setMBeanServer(MBeanServer beanServer) {
        this.beanServer = beanServer;
    }

    /**
     * @return Returns the useMBeanServer.
     */
    public boolean isUseMBeanServer() {
        return useMBeanServer;
    }

    /**
     * @param useMBeanServer The useMBeanServer to set.
     */
    public void setUseMBeanServer(boolean useMBeanServer) {
        this.useMBeanServer = useMBeanServer;
    }

    /**
     * @return Returns the createMBeanServer flag.
     */
    public boolean isCreateMBeanServer() {
        return createMBeanServer;
    }

    /**
     * @param enableJMX Set createMBeanServer.
     */
    public void setCreateMBeanServer(boolean enableJMX) {
        this.createMBeanServer = enableJMX;
    }

    public boolean isFindTigerMbeanServer() {
        return findTigerMbeanServer;
    }

    public boolean isConnectorStarted() {
        return connectorStarting.get() || (connectorServer != null && connectorServer.isActive());
    }

    /**
     * Enables/disables the searching for the Java 5 platform MBeanServer
     */
    public void setFindTigerMbeanServer(boolean findTigerMbeanServer) {
        this.findTigerMbeanServer = findTigerMbeanServer;
    }

    /**
     * Formulate and return the MBean ObjectName of a custom control MBean
     *
     * @param type
     * @param name
     * @return the JMX ObjectName of the MBean, or <code>null</code> if
     *         <code>customName</code> is invalid.
     */
    public ObjectName createCustomComponentMBeanName(String type, String name) {
        ObjectName result = null;
        String tmp = jmxDomainName + ":" + "type=" + sanitizeString(type) + ",name=" + sanitizeString(name);
        try {
            result = new ObjectName(tmp);
        } catch (MalformedObjectNameException e) {
            LOG.error("Couldn't create ObjectName from: " + type + " , " + name);
        }
        return result;
    }

    /**
     * The ':' and '/' characters are reserved in ObjectNames
     *
     * @param in
     * @return sanitized String
     */
    private static String sanitizeString(String in) {
        String result = null;
        if (in != null) {
            result = in.replace(':', '_');
            result = result.replace('/', '_');
            result = result.replace('\\', '_');
        }
        return result;
    }

    /**
     * Retrieve an System ObjectName
     *
     * @param domainName
     * @param containerName
     * @param theClass
     * @return the ObjectName
     * @throws MalformedObjectNameException
     */
    public static ObjectName getSystemObjectName(String domainName, String containerName, Class<?> theClass) throws MalformedObjectNameException, NullPointerException {
        String tmp = domainName + ":" + "type=" + theClass.getName() + ",name=" + getRelativeName(containerName, theClass);
        return new ObjectName(tmp);
    }

    private static String getRelativeName(String containerName, Class<?> theClass) {
        String name = theClass.getName();
        int index = name.lastIndexOf(".");
        if (index >= 0 && (index + 1) < name.length()) {
            name = name.substring(index + 1);
        }
        return containerName + "." + name;
    }

    public Object newProxyInstance(ObjectName objectName, Class<?> interfaceClass, boolean notificationBroadcaster){
        return MBeanServerInvocationHandler.newProxyInstance(getMBeanServer(), objectName, interfaceClass, notificationBroadcaster);
    }

    public Object getAttribute(ObjectName name, String attribute) throws Exception{
        return getMBeanServer().getAttribute(name, attribute);
    }

    public ObjectInstance registerMBean(Object bean, ObjectName name) throws Exception{
        ObjectInstance result = getMBeanServer().registerMBean(bean, name);
        this.registeredMBeanNames.put(name, result.getObjectName());
        return result;
    }

    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) throws Exception{
        if (name != null) {
            ObjectName actualName = this.registeredMBeanNames.get(name);
            if (actualName != null) {
                return getMBeanServer().queryNames(actualName, query);
            }
        }
        return getMBeanServer().queryNames(name, query);
    }

    public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
        return getMBeanServer().getObjectInstance(name);
    }

    /**
     * Unregister an MBean
     *
     * @param name
     * @throws JMException
     */
    public void unregisterMBean(ObjectName name) throws JMException {
        ObjectName actualName = this.registeredMBeanNames.get(name);
        if (beanServer != null && actualName != null && beanServer.isRegistered(actualName) && this.registeredMBeanNames.remove(name) != null) {
            LOG.debug("Unregistering MBean {}", actualName);
            beanServer.unregisterMBean(actualName);
        }
    }

    protected synchronized MBeanServer findMBeanServer() {
        MBeanServer result = null;

        try {
            if (useMBeanServer) {
                if (findTigerMbeanServer) {
                    result = findTigerMBeanServer();
                }
                if (result == null) {
                    // lets piggy back on another MBeanServer - we could be in an appserver!
                    List<MBeanServer> list = MBeanServerFactory.findMBeanServer(null);
                    if (list != null && list.size() > 0) {
                        result = list.get(0);
                    }
                }
            }
            if (result == null && createMBeanServer) {
                result = createMBeanServer();
            }
        } catch (NoClassDefFoundError e) {
            LOG.error("Could not load MBeanServer", e);
        } catch (Throwable e) {
            // probably don't have access to system properties
            LOG.error("Failed to initialize MBeanServer", e);
        }
        return result;
    }

    public MBeanServer findTigerMBeanServer() {
        String name = "java.lang.management.ManagementFactory";
        Class<?> type = loadClass(name, ManagementContext.class.getClassLoader());
        if (type != null) {
            try {
                Method method = type.getMethod("getPlatformMBeanServer", new Class[0]);
                if (method != null) {
                    Object answer = method.invoke(null, new Object[0]);
                    if (answer instanceof MBeanServer) {
                        if (createConnector) {
                            createConnector((MBeanServer)answer);
                        }
                        return (MBeanServer)answer;
                    } else {
                        LOG.warn("Could not cast: " + answer + " into an MBeanServer. There must be some classloader strangeness in town");
                    }
                } else {
                    LOG.warn("Method getPlatformMBeanServer() does not appear visible on type: " + type.getName());
                }
            } catch (Exception e) {
                LOG.warn("Failed to call getPlatformMBeanServer() due to: " + e, e);
            }
        } else {
            LOG.trace("Class not found: " + name + " so probably running on Java 1.4");
        }
        return null;
    }

    private static Class<?> loadClass(String name, ClassLoader loader) {
        try {
            return loader.loadClass(name);
        } catch (ClassNotFoundException e) {
            try {
                return Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e1) {
                return null;
            }
        }
    }

    /**
     * @return
     * @throws NullPointerException
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    protected MBeanServer createMBeanServer() throws MalformedObjectNameException, IOException {
        MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer(jmxDomainName);
        locallyCreateMBeanServer = true;
        if (createConnector) {
        	createConnector(mbeanServer);
        }
        return mbeanServer;
    }

    /**
     * @param mbeanServer
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    private void createConnector(MBeanServer mbeanServer) throws MalformedObjectNameException, IOException {
        // Create the NamingService, needed by JSR 160
        try {
            if (registry == null) {
                LOG.debug("Creating RMIRegistry on port {}", connectorPort);
                registry = LocateRegistry.createRegistry(connectorPort);
            }
            
            namingServiceObjectName = ObjectName.getInstance("naming:type=rmiregistry");

            // Do not use the createMBean as the mx4j jar may not be in the
            // same class loader than the server
            Class<?> cl = Class.forName("mx4j.tools.naming.NamingService");
            mbeanServer.registerMBean(cl.newInstance(), namingServiceObjectName);

            // set the naming port
            Attribute attr = new Attribute("Port", Integer.valueOf(connectorPort));
            mbeanServer.setAttribute(namingServiceObjectName, attr);
        } catch(ClassNotFoundException e) {
            LOG.debug("Probably not using JRE 1.4: " + e.getLocalizedMessage());
        } catch (Throwable e) {
            LOG.debug("Failed to create local registry. This exception will be ignored.", e);
        }

        // Create the JMXConnectorServer
        String rmiServer = "";
        if (rmiServerPort != 0) {
            // This is handy to use if you have a firewall and need to force JMX to use fixed ports.
            rmiServer = ""+getConnectorHost()+":" + rmiServerPort;
        }
        String serviceURL = "service:jmx:rmi://" + rmiServer + "/jndi/rmi://" +getConnectorHost()+":" + connectorPort + connectorPath;
        JMXServiceURL url = new JMXServiceURL(serviceURL);
        connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, environment, mbeanServer);

        LOG.info("Created JMXConnectorServer {}", connectorServer);
    }

    public String getConnectorPath() {
        return connectorPath;
    }

    public void setConnectorPath(String connectorPath) {
        this.connectorPath = connectorPath;
    }

    public int getConnectorPort() {
        return connectorPort;
    }

   public void setConnectorPort(int connectorPort) {
        this.connectorPort = connectorPort;
    }

    public int getRmiServerPort() {
        return rmiServerPort;
    }

    public void setRmiServerPort(int rmiServerPort) {
        this.rmiServerPort = rmiServerPort;
    }

    public boolean isCreateConnector() {
        return createConnector;
    }

    public void setCreateConnector(boolean createConnector) {
        this.createConnector = createConnector;
    }

    /**
     * Get the connectorHost
     * @return the connectorHost
     */
    public String getConnectorHost() {
        return this.connectorHost;
    }

    /**
     * Set the connectorHost
     * @param connectorHost the connectorHost to set
     */
    public void setConnectorHost(String connectorHost) {
        this.connectorHost = connectorHost;
    }

    public Map<String, ?> getEnvironment() {
        return environment;
    }

    public void setEnvironment(Map<String, ?> environment) {
        this.environment = environment;
    }

    public boolean isAllowRemoteAddressInMBeanNames() {
        return allowRemoteAddressInMBeanNames;
    }

    public void setAllowRemoteAddressInMBeanNames(boolean allowRemoteAddressInMBeanNames) {
        this.allowRemoteAddressInMBeanNames = allowRemoteAddressInMBeanNames;
    }

	@Override
	public void doInit() {
		LOG.info(" management context do init");
	}
}
