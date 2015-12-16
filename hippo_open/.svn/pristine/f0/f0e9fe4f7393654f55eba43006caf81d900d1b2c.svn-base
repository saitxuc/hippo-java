package com.pinganfu.hippo.broker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.plugin.BrokerPlugin;
import com.pinganfu.hippo.broker.transport.HippoBrokerCommandManager;
import com.pinganfu.hippo.broker.transport.HippoTransportConnectionManager;
import com.pinganfu.hippo.broker.transport.TransportConnector;
import com.pinganfu.hippo.broker.transport.TransportConnectorFactory;
import com.pinganfu.hippo.broker.useage.SystemUsage;
import com.pinganfu.hippo.client.ClientConstants;
import com.pinganfu.hippo.client.HippoResult;
import com.pinganfu.hippo.client.command.AtomicntCommand;
import com.pinganfu.hippo.client.command.GetBitCommand;
import com.pinganfu.hippo.client.command.GetCommand;
import com.pinganfu.hippo.client.command.RemoveCommand;
import com.pinganfu.hippo.client.command.SetBitCommand;
import com.pinganfu.hippo.client.command.SetCommand;
import com.pinganfu.hippo.client.command.UpdateCommand;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.common.util.HashUtil;
import com.pinganfu.hippo.common.util.IOExceptionSupport;
import com.pinganfu.hippo.jmx.AnnotatedMBean;
import com.pinganfu.hippo.jmx.BrokerMBeanSupport;
import com.pinganfu.hippo.jmx.BrokerView;
import com.pinganfu.hippo.jmx.ConnectorView;
import com.pinganfu.hippo.jmx.ConnectorViewMBean;
import com.pinganfu.hippo.jmx.ManagedConnection;
import com.pinganfu.hippo.jmx.ManagedTransportConnection;
import com.pinganfu.hippo.jmx.ManagedTransportConnector;
import com.pinganfu.hippo.manager.ManagementContext;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;
import com.pinganfu.hippo.network.transport.TransportConnectionManager;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 *
 */
public class BrokerService extends LifeCycleSupport implements Broker {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerService.class);
    private static final String DEFAULT_BROKER_NAME = "hippo-broker";
    protected String brokerName = DEFAULT_BROKER_NAME;
    private ManagementContext managementContext;
    private BrokerView adminView;
    private ObjectName brokerObjectName;
    private CommandManager commandManager;
    private BrokerPlugin[] plugins;
    private final CountDownLatch startedLatch = new CountDownLatch(1);
    private List<Runnable> shutdownHooks = new ArrayList<Runnable>();
    private List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
    private List registeredMBeanNames = new CopyOnWriteArrayList();
    protected String brokerUris = null;
    private ThreadPoolExecutor executor = null;
    private final Object storeEngineLock = new Object();
    private Throwable startException = null;
    private boolean useShutdownHook = true;
    private boolean enableStatistics = true;
    private File dataDirectoryFile = null;
    private boolean isSimpleMode = false;
    protected boolean useJmx = false;
    protected String jmxConnectorHost = null;
    protected Cache cache;
    private List<TransportConnector> transportConnectors = new CopyOnWriteArrayList<TransportConnector>();

    public void setTransportConnectors(List<TransportConnector> transportConnectors) {
        this.transportConnectors = transportConnectors;
    }

    protected Map<String, String> configMap = new HashMap<String, String>();
    protected Serializer serializer = null;

    private transient Thread shutdownHook;
    private SystemUsage systemUsage;

    private String nioType = null;

    private static final int MUTEX_ARRAY_SIZE = 1000;
    private Object[] counterMutex = new Object[MUTEX_ARRAY_SIZE];

    @Override
    public void doInit() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService is doing doInit. ");
        }
        try {
            if (cache == null) {
                cache = new DefaultCache(this);
            }
            cache.init();

        } catch (Exception e) {
            LOG.error(" Broker do init happen failure ", e);

        }
        if (commandManager == null) {
            commandManager = new HippoBrokerCommandManager(this);
        }
        if (serializer == null) {
            serializer = new KryoSerializer();
        }
        try {
            addInterceptors(this);
        } catch (Exception e) {
            throw new RuntimeException(" install broker plugins happen error, ", e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService finsih doInit. ");
        }
    }

    @Override
    public void doStart() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService is doing doStart. ");
        }
        try {
            if (isUseJmx()) {
                MDC.remove("hippo.broker");
                try {
                    startManagementContext();
                } finally {
                    MDC.put("hippo.broker", brokerName);
                }
            }
            startCacheEngine(false);
            startBroker(false);
            checkSystemUsageLimits();
            //addShutdownHook();

            if (isSimpleMode && cache.getEngine() instanceof MdbStoreEngine) {
                //单机模式启动存储引擎自己的过期收集
                ((MdbStoreEngine) cache.getEngine()).startPeriodsExpire();
                LOG.info("start the periods expire thread successfully");
            }
        } catch (Exception e) {
            LOG.error("Failed to start hippo (" + getBrokerName() + "). Reason: " + e, e);
            try {
                stop();
            } catch (Exception ex) {
                LOG.warn("Failed to stop broker after failure in start. This exception will be ignored.", ex);
            }
            //throw e;
        } finally {
            MDC.remove("hippo.broker");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService finsih doStart. ");
        }
    }

    @Override
    public void doStop() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService is doing doStop. ");
        }
        removeShutdownHook();
        
        cache.stop();
        for (int i = 0; i < transportConnectors.size(); i++) {
            TransportConnector connector = transportConnectors.get(i);
            connector.stop();
        }
        this.stopLifeCycles();

        if (isUseJmx()) {
            MBeanServer mbeanServer = getManagementContext().getMBeanServer();
            if (mbeanServer != null) {
                for (Iterator iter = registeredMBeanNames.iterator(); iter.hasNext();) {
                    ObjectName name = (ObjectName) iter.next();
                    try {
                        mbeanServer.unregisterMBean(name);
                    } catch (Exception e) {
                        //stopper.onException(, e);
                    }
                }
            }
            getManagementContext().stop();
        }
        getSystemUsage().stop();
        if (this.executor != null) {
            this.executor.shutdownNow();
        }
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService finish doStop. ");
        }
    }

    @Override
    public void startCacheEngine(boolean startAsync) {
        if (startAsync) {
            new Thread("Broker Starting Thread") {
                @Override
                public void run() {
                    try {
                        cache.start();
                    } catch (Throwable t) {
                        startException = t;
                    } finally {
                        synchronized (storeEngineLock) {
                            storeEngineLock.notifyAll();
                        }
                    }
                }
            }.start();
        } else {
            cache.start();
        }

    }

    @Override
    public void startBroker(boolean startAsync) {
        if (startException != null) {
            return;
        }
        if (startAsync) {
            new Thread("Broker Starting Thread") {
                @Override
                public void run() {
                    try {
                        synchronized (storeEngineLock) {
                            storeEngineLock.wait();
                        }
                        doStartBroker();
                    } catch (Throwable t) {
                        startException = t;
                    }
                }
            }.start();
        } else {
            doStartBroker();
        }

    }

    @Override
    public TransportConnector addConnector(String shema, int bindPort) throws Exception {
        TransportConnector connector = TransportConnectorFactory.create(nioType, shema, bindPort, commandManager, serializer);
        connector.setBrokerService(this);
        transportConnectors.add(connector);
        return connector;
    }

    @Override
    public TransportConnector addConnector(String protocal, int bindPort, int maxconnections) throws Exception {
        TransportConnector connector = this.addConnector(protocal, bindPort);
        connector.setMaximumConnections(maxconnections);
        return connector;
    }

    public HippoResult processSet(SetCommand setCommand) {
        int expire = setCommand.getExpire();
        String bucketNo = setCommand.getHeadValue(ClientConstants.HEAD_BUCKET_NO);
        int version = Integer.parseInt(setCommand.getHeadValue(ClientConstants.HEAD_VERSION));
        byte[] source = setCommand.getData();
        int klength = setCommand.getKlength();
        final byte[] keybytes = new byte[klength];
        System.arraycopy(source, 0, keybytes, 0, klength);

        final byte[] databytes = new byte[source.length - klength];
        System.arraycopy(source, klength, databytes, 0, databytes.length);
        
        HippoResult result = cache.set(expire, keybytes, databytes, version, Integer.parseInt(bucketNo));
        return result;
    }
    
    public HippoResult processUpdate(UpdateCommand updateCommand) {
        int expire = updateCommand.getExpire();
        String bucketNo = updateCommand.getHeadValue(ClientConstants.HEAD_BUCKET_NO);
        int version = Integer.parseInt(updateCommand.getHeadValue(ClientConstants.HEAD_VERSION));
        byte[] source = updateCommand.getData();
        int klength = updateCommand.getKlength();
        final byte[] keybytes = new byte[klength];
        System.arraycopy(source, 0, keybytes, 0, klength);
        final byte[] databytes = new byte[source.length - klength];
        System.arraycopy(source, klength, databytes, 0, databytes.length);
        HippoResult result = cache.update(expire, keybytes, databytes, version, Integer.parseInt(bucketNo));
        return result;
    }
    
    protected void checkSystemUsageLimits() throws IOException {
        /***
        SystemUsage usage = getSystemUsage();
        long memLimit = usage.getMemoryUsage().getLimit();
        long jvmLimit = Runtime.getRuntime().maxMemory();
        if (memLimit > jvmLimit) {
            LOG.error("Memory Usage for the Broker (" + memLimit / (1024 * 1024) +
                      " mb) is more than the maximum available for the JVM: " +
                      jvmLimit / (1024 * 1024) + " mb");
        }
        ***/
        if (this.getCache() != null) {
            Cache cache = getCache();
            cache.checkStoreEngineConfig();
        }
    }

    public void setCommandManager(CommandManager commandManager) {
        this.commandManager = commandManager;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public HippoResult processGet(GetCommand getCommand) {
        String bucketNo = getCommand.getHeadValue(ClientConstants.HEAD_BUCKET_NO);
        byte[] keybytes = getCommand.getData();
        HippoResult result = cache.get(keybytes, Integer.parseInt(bucketNo));
        return result;
    }

    public HippoResult processRemove(RemoveCommand removeCommand) {
        String bucketNo = removeCommand.getHeadValue(ClientConstants.HEAD_BUCKET_NO);
        byte[] keybytes = removeCommand.getData();
        HippoResult result = cache.remove(keybytes, Integer.parseInt(bucketNo));
        return result;
    }

    private Object getCounterMutex(byte[] data) {
        int hash = HashUtil.murmurhash2(data, MUTEX_ARRAY_SIZE);
        int index = Math.abs(hash) % MUTEX_ARRAY_SIZE;
        if (counterMutex[index] == null) {
            counterMutex[index] = new Object();
        }
        return counterMutex[index];
    }

    public HippoResult processAtomicnt(AtomicntCommand atomicntCommand) {
        byte[] keybytes = atomicntCommand.getData();
        String bucketNo = atomicntCommand.getHeadValue(ClientConstants.HEAD_BUCKET_NO);
        boolean checkEdge = Boolean.parseBoolean(atomicntCommand.getHeadValue("checkValEdge"));
        synchronized (getCounterMutex(keybytes)) {
            HippoResult result = cache.get(keybytes, Integer.parseInt(bucketNo));
            try {
                long delta = atomicntCommand.getDelta();
                long newv = -1;
                boolean isNew = false;
                if (result.isSuccess()) {
                    //get the expire time
                    String expireTimeStr = result.getSttribute("expireTime");
                    if (StringUtils.isNotEmpty(expireTimeStr) && Long.parseLong(expireTimeStr) > 0 && System.currentTimeMillis() > Long
                        .parseLong(expireTimeStr)) {
                        newv = atomicntCommand.getInitv() + delta;
                        isNew = true;
                    } else {
                        byte[] databytes = result.getData();
                        long oldv = serializer.deserialize(databytes, Long.class);
                        newv = oldv + delta;
                    }
                } else {
                    newv = atomicntCommand.getInitv() + delta;
                    isNew = true;
                }

                if (checkEdge) {
                    if (newv > Long.MAX_VALUE) {
                        throw new HippoStoreException("the number after modify will larger than the Long max value, the opertion failed, key -> " + new String(keybytes));
                    } else if (newv < Long.MIN_VALUE) {
                        throw new HippoStoreException("the number after modify will smaller than the Long min value, the opertion failed, key -> " + new String(keybytes));
                    }
                }

                byte[] content = serializer.serialize(newv);

                if (isNew) {
                    result = cache.set(atomicntCommand.getExpire(), keybytes, content, Integer.parseInt(bucketNo));
                } else {
                    result = cache.update(atomicntCommand.getExpire(), keybytes, content, 0, Integer.parseInt(bucketNo));
                }

                result.setData(content);
            } catch (Exception e) {
                LOG.error("error in processAtomicnt!", e);
                return new HippoResult(false, HippoCodeDefine.HIPPO_ATOMIC_OPER_ERROR, e.getMessage());
            }
            return result;
        }
    }

    @Override
    public HippoResult processCommand(Command command) {
        if (isSimpleMode) {
            command.getHeaders().put(ClientConstants.HEAD_BUCKET_NO, ReplicatedConstants.DEFAULT_BUCKET_NO);
        }
        String action = command.getAction();
        if (CommandConstants.SET_COMMAND_ACTION.equals(action)) {
            return processSet((SetCommand) command);
        }
        if (CommandConstants.UPDATE_COMMAND_ACTION.equals(action)) {
            return processUpdate((UpdateCommand) command);
        }
        if (CommandConstants.GET_COMMAND_ACTION.equals(action)) {
            return processGet((GetCommand) command);
        }
        if (CommandConstants.REMOVE_COMMAND_ACTION.equals(action)) {
            return processRemove((RemoveCommand) command);
        }
        if (CommandConstants.ATOMICNT_COMMAND_ACTION.equals(action)) {
            return processAtomicnt((AtomicntCommand) command);
        }
        if (CommandConstants.BITSET_COMMAND_ACTION.equals(action)) {
            return processSetBit((SetBitCommand) command);
        }
        if (CommandConstants.BITGET_COMMAND_ACTION.equals(action)) {
            return processGetBit((GetBitCommand) command);
        }
        return null;
    }

    private HippoResult processGetBit(GetBitCommand command) {
        byte[] keybytes = command.getData();
        HippoResult result = null;
        String offsetStr = command.getHeadValue(CommandConstants.BIT_OFFSET);
        String bucketNo = command.getHeadValue(ClientConstants.HEAD_BUCKET_NO);

        if (Boolean.parseBoolean(command.getHeadValue(CommandConstants.BIT_WHOLEGET))) {
            result = cache.get(keybytes, Integer.parseInt(bucketNo));
        } else {
            int offset = Integer.parseInt(offsetStr);
            result = cache.getBit(keybytes, offset, Integer.parseInt(bucketNo));
        }

        if (!result.isSuccess() && (HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST.equals(result.getErrorCode()) || HippoCodeDefine.HIPPO_DATA_EXPIRED.equals(result
            .getErrorCode()))) {
            result.setSuccess(true);
            result.putAttribute(CommandConstants.BIT_NOT_EXIST, "true");
            result.putAttribute(CommandConstants.BIT_GET_EXT_CODE, result.getErrorCode());
        }
        return result;
    }

    private HippoResult processSetBit(SetBitCommand command) {
        int expire = command.getExpire();
        byte[] key = command.getData();
        String offsetStr = command.getHeadValue(CommandConstants.BIT_OFFSET);
        int offset = Integer.parseInt(offsetStr);

        if (offset > Integer.MAX_VALUE || offset < 0) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_DATA_OUT_RANGE, "bit data out of range");
        }

        String bucketNo = command.getHeadValue(ClientConstants.HEAD_BUCKET_NO);

        boolean val = Boolean.parseBoolean(command.getHeadValue(CommandConstants.BIT_VAL));

        return cache.setBit(expire, key, offset, val, Integer.parseInt(bucketNo));
    }

    public void doStartBroker() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService doing start broker server. ");
        }
        startAllConnector();
        startLifeCycles();
        if (LOG.isInfoEnabled()) {
            LOG.info(" BrokerService finish start broker server. ");
        }
    }

    private void startAllConnector() {
        try {
            for (int i = 0; i < transportConnectors.size(); i++) {
                TransportConnector connector = transportConnectors.get(i);
                connector.setBrokerService(this);
                this.startTransportConnector(connector);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void startLifeCycles() {
        for (int i = 0; i < lifeCycles.size(); i++) {
            LifeCycle lifeCycle = lifeCycles.get(i);
            lifeCycle.start();
        }
    }

    public void stopLifeCycles() {
        for (int i = 0; i < lifeCycles.size(); i++) {
            LifeCycle lifeCycle = lifeCycles.get(i);
            lifeCycle.stop();
        }
    }

    protected void startManagementContext() throws Exception {
        getManagementContext().setBrokerName(brokerName);
        getManagementContext().start();
        adminView = new BrokerView(this);
        ObjectName objectName = getBrokerObjectName();
        AnnotatedMBean.registerMBean(getManagementContext(), adminView, objectName);
    }

    protected ObjectName createBrokerObjectName() throws MalformedObjectNameException {
        return BrokerMBeanSupport.createBrokerObjectName(getManagementContext().getJmxDomainName(), getBrokerName());
    }

    protected void addShutdownHook() {
        if (useShutdownHook) {
            shutdownHook = new Thread("Hippo ShutdownHook") {
                @Override
                public void run() {
                    containerShutdown();
                }
            };
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    protected void containerShutdown() {
        try {
            stop();
        } catch (Exception e) {
            LOG.error("Failed to shut down: " + e, e);
        }
    }

    protected void removeShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (Exception e) {
                LOG.debug("Caught exception, must be shutting down: " + e);
            }
        }
    }

    protected synchronized ThreadPoolExecutor getExecutor() {
        if (this.executor == null) {
            this.executor = new ThreadPoolExecutor(1, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {

                private long i = 0;

                @Override
                public Thread newThread(Runnable runnable) {
                    this.i++;
                    Thread thread = new Thread(runnable, "hippo BrokerService.worker." + this.i);
                    thread.setDaemon(true);
                    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(final Thread t, final Throwable e) {
                            LOG.error("Error in thread '{}'", t.getName(), e);
                        }
                    });
                    return thread;
                }
            }, new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
                    try {
                        executor.getQueue().offer(r, 60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        throw new RejectedExecutionException("Interrupted waiting for BrokerService.worker");
                    }

                    throw new RejectedExecutionException("Timed Out while attempting to enqueue Task.");
                }
            });
        }
        return this.executor;
    }

    protected TransportConnector startTransportConnector(TransportConnector connector) throws Exception {
        if (isUseJmx()) {
            connector = registerConnectorMBean(connector);
        }
        connector.getStatistics().setEnabled(enableStatistics);
        connector.getServer().setTransportConnectionManager(createTransportConnectionManager(connector));
        connector.start();
        return connector;
    }

    public ManagementContext getManagementContext() {
        if (managementContext == null) {
            managementContext = new ManagementContext();
        }
        managementContext.setConnectorHost(this.jmxConnectorHost);
        return managementContext;
    }

    public void setShutdownHooks(List<Runnable> hooks) throws Exception {
        for (Runnable hook : hooks) {
            addShutdownHook(hook);
        }
    }

    public void addShutdownHook(Runnable hook) {
        synchronized (shutdownHooks) {
            shutdownHooks.add(hook);
        }
    }

    public void removeShutdownHook(Runnable hook) {
        synchronized (shutdownHooks) {
            shutdownHooks.remove(hook);
        }
    }

    @Override
    public String getUptime() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void gc() {

    }

    @Override
    public List<TransportConnector> getTransportConnectors() {
        return transportConnectors;
    }

    @Override
    public TransportConnector getConnectorByName(String name) {
        for (TransportConnector transportConnector : transportConnectors) {
            if (transportConnector.getName().equals(name)) {
                return transportConnector;
            }
        }
        return null;
    }

    @Override
    public boolean removeConnector(TransportConnector connector) {
        for (TransportConnector transportConnector : transportConnectors) {
            if (transportConnector.getName().equals(connector.getName())) {
                return transportConnectors.remove(transportConnector);
            }
        }
        return false;
    }

    @Override
    public File getDataDirectoryFile() {
        if (dataDirectoryFile == null) {
            dataDirectoryFile = new File(IOHelper.getDefaultDataDirectory());
        }
        return dataDirectoryFile;
    }

    public File getBrokerDataDirectory() {
        String brokerDir = getBrokerName();
        return new File(getDataDirectoryFile(), brokerDir);
    }

    public void setDataDirectory(String dataDirectory) {
        setDataDirectoryFile(new File(dataDirectory));
    }

    public void setDataDirectoryFile(File dataDirectoryFile) {
        this.dataDirectoryFile = dataDirectoryFile;
    }

    public boolean waitUntilStarted() {
        boolean waitSucceeded = isStarted();
        while (!isStarted() && !this.stoped.get() && !waitSucceeded) {
            try {
                if (startException != null) {
                    return waitSucceeded;
                }
                waitSucceeded = startedLatch.await(100L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignore) {
            }
        }
        return waitSucceeded;
    }

    @Override
    public SystemUsage getSystemUsage() {
        try {
            if (systemUsage == null) {

                systemUsage = new SystemUsage("Main", cache.getEngine());
                systemUsage.setExecutor(getExecutor());
                systemUsage.getMemoryUsage().setLimit(1024 * 1024 * 512); // 64 MB
                systemUsage.getStoreUsage().setLimit(1024L * 1024 * 1024 * 100); // 100 GB
                addLifeCycle(this.systemUsage);
            }
            return systemUsage;
        } catch (Exception e) {
            LOG.error("Cannot create SystemUsage", e);
            throw new RuntimeException("Fatally failed to create SystemUsage" + e.getMessage());
        }
    }

    protected TransportConnector registerConnectorMBean(TransportConnector connector) throws IOException {
        try {
            ObjectName objectName = createConnectorObjectName(connector);
            connector = connector.asManagedConnector(getManagementContext(), objectName);
            ConnectorViewMBean view = new ConnectorView(connector);
            AnnotatedMBean.registerMBean(getManagementContext(), view, objectName);
            return connector;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Transport Connector could not be registered in JMX: " + e.getMessage(), e);
        }
    }

    protected void addInterceptors(Broker broker) throws Exception {
        if (plugins != null) {
            for (int i = 0; i < plugins.length; i++) {
                BrokerPlugin plugin = plugins[i];
                broker = plugin.installPlugin(broker);
            }
        }
    }

    public void setSystemUsage(SystemUsage memoryManager) {
        if (this.systemUsage != null) {
            removeLifeCycle(this.systemUsage);
        }
        this.systemUsage = memoryManager;
        if (this.systemUsage.getExecutor() == null) {
            this.systemUsage.setExecutor(getExecutor());
        }
        addLifeCycle(this.systemUsage);
    }

    public void addLifeCycle(LifeCycle lifeCycle) {
        lifeCycles.add(lifeCycle);
    }

    public void removeLifeCycle(LifeCycle lifeCycle) {
        lifeCycles.remove(lifeCycle);
    }

    public boolean isUseShutdownHook() {
        return useShutdownHook;
    }

    public void setUseShutdownHook(boolean useShutdownHook) {
        this.useShutdownHook = useShutdownHook;
    }

    public boolean isUseJmx() {
        return useJmx;
    }

    public void setUseJmx(boolean useJmx) {
        this.useJmx = useJmx;
    }

    public boolean isEnableStatistics() {
        return enableStatistics;
    }

    public CommandManager getCommandManager() {
        return commandManager;
    }

    /**
     * Sets whether or not the Broker's services enable statistics or not.
     */
    public void setEnableStatistics(boolean enableStatistics) {
        this.enableStatistics = enableStatistics;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public ObjectName getBrokerObjectName() throws MalformedObjectNameException {
        if (brokerObjectName == null) {
            brokerObjectName = createBrokerObjectName();
        }
        return brokerObjectName;
    }

    public void setBrokerObjectName(ObjectName brokerObjectName) {
        this.brokerObjectName = brokerObjectName;
    }

    private ObjectName createConnectorObjectName(TransportConnector connector) throws MalformedObjectNameException {
        return BrokerMBeanSupport.createConnectorName(getBrokerObjectName(), "clientConnectors", connector.getName());
    }

    public Cache getCache() {
        return cache;
    }

    @Override
    public Map<String, String> getConfigMap() {
        return configMap;
    }

    public String getNioType() {
        return nioType;
    }

    public void setNioType(String nioType) {
        this.nioType = nioType;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public boolean isSimpleMode() {
        return isSimpleMode;
    }

    public void setSimpleMode(boolean isSimpleMode) {
        this.isSimpleMode = isSimpleMode;
    }

    public void setLimit(long limit) {
        if (cache == null) {
            cache = new DefaultCache(this);
        }
        cache.setLimit(limit);
    }

    public void setBuckets(List<BucketInfo> buckets) {
        if (cache == null) {
            cache = new DefaultCache(this);
        }
        cache.setBuckets(buckets);
    }

    @Override
    public String getStoreEngineAdapterObjectName() throws Exception {
        return BrokerMBeanSupport.StoreEngineAdapterObjectName;
    }

    @Override
    public List<String> getConnectorName() throws Exception {
        List<String> list = new ArrayList<String>();
        List<TransportConnector> connectorList = this.transportConnectors;
        if (connectorList != null && connectorList.size() > 0) {
            for (int i = 0; i < connectorList.size(); i++) {
                TransportConnector transportConn = (TransportConnector) connectorList.get(i);
                ServerFortress serverFortress = (ServerFortress) transportConn.getServer();
                HippoTransportConnectionManager hippoConnectionManager = (HippoTransportConnectionManager) serverFortress.getTransportConnectionManager();
                ManagedTransportConnector managedTran = (ManagedTransportConnector) hippoConnectionManager.getConnector();
                list.add(managedTran.getConnectorName().toString());
            }
        }
        return list;
    }

    @Override
    public List<Map<String, String>> getClientObjectNames() throws Exception {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        List<TransportConnector> connectorList = this.transportConnectors;
        if (connectorList != null && connectorList.size() > 0) {
            for (int i = 0; i < connectorList.size(); i++) {
                TransportConnector transportConn = (TransportConnector) connectorList.get(i);
                HippoTransportConnectionManager hippoConnectionManager = (HippoTransportConnectionManager) transportConn.getServer()
                    .getTransportConnectionManager();
                ManagedTransportConnector managedTran = (ManagedTransportConnector) hippoConnectionManager.getConnector();
                String objNameKey = managedTran.getConnectorName().toString();
                HippoTransportConnectionManager hippoClientConnectionManager = (HippoTransportConnectionManager) managedTran.getServer()
                    .getTransportConnectionManager();
                List<ManagedConnection> conList = hippoClientConnectionManager.getManagedConnections();
                if (conList != null && conList.size() > 0) {
                    for (ManagedConnection mconn : conList) {
                        ManagedTransportConnection clientConn = (ManagedTransportConnection) mconn;
                        Map<String, String> map = new HashMap<String, String>();
                        map.put(objNameKey, clientConn.getByAddressName());
                        list.add(map);
                    }
                }
            }
        }
        return list;
    }

    @Override
    public TransportConnectionManager createTransportConnectionManager(TransportConnector connector) {
        TransportConnectionManager tcmanager = new HippoTransportConnectionManager(connector);
        return tcmanager;
    }

    @Override
    public void setPlugins(BrokerPlugin[] plugins) {
        this.plugins = plugins;
    }

    public String getJmxConnectorHost() {
        return jmxConnectorHost;
    }

    public void setJmxConnectorHost(String jmxConnectorHost) {
        this.jmxConnectorHost = jmxConnectorHost;
    }

    @Override
    public void setBrokerUris(String brokerUris) {
        this.brokerUris = brokerUris;
    }

}
