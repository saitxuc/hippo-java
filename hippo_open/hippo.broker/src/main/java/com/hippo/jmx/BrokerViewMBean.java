package com.hippo.jmx;

import java.util.List;
import java.util.Map;

import com.hippo.common.lifecycle.LifeCycle;


/**
 * 
 * @author saitxuc
 * write 2014-8-12
 */
public interface BrokerViewMBean extends LifeCycle {

    /**
     * @return The name of the broker.
     */
    @MBeanInfo("The name of the broker.")
    String getBrokerName();

    /**
     * @return The name of the broker.
     */
    @MBeanInfo("The version of the broker.")
    String getBrokerVersion();

    /**
     * @return Uptime of the broker.
     */
    @MBeanInfo("Uptime of the broker.")
    String getUptime();

    /**
     * The Broker will flush it's caches so that the garbage collector can
     * reclaim more memory.
     *
     * @throws Exception
     */
    @MBeanInfo("Runs the Garbage Collector.")
    void gc() throws Exception;

    @MBeanInfo("Percent of memory limit used.")
    int getMemoryPercentUsage();

    @MBeanInfo("Memory limit, in bytes, used for holding undelivered messages before paging to temporary storage.")
    long getMemoryLimit();

    void setMemoryLimit(@MBeanInfo("bytes") long limit);

    @MBeanInfo("Percent of store limit used.")
    int getStorePercentUsage();

    @MBeanInfo("Disk limit, in bytes, used for persistent messages before producers are blocked.")
    long getStoreLimit();

    void setStoreLimit(@MBeanInfo("bytes") long limit);

    /**
     * Shuts down the JVM.
     *
     * @param exitCode the exit code that will be reported by the JVM process
     *                when it exits.
     */
    @MBeanInfo("Shuts down the JVM.")
    void terminateJVM(@MBeanInfo("exitCode") int exitCode);

    /**
     * Stop the broker and all it's components.
     */
    @MBeanInfo("Stop the broker and all its components.")
    void stop();
    
    @MBeanInfo("Adds a Connector to the broker.")
    String addConnector(@MBeanInfo("discoveryAddress") String protocal, @MBeanInfo("bindPort")int bindPort) throws Exception;

    @MBeanInfo("Removes a Connector from the broker.")
    boolean removeConnector(@MBeanInfo("connectorName") String connectorName) throws Exception;


    @MBeanInfo(value="Reloads log4j.properties from the classpath.")
    public void reloadLog4jProperties() throws Throwable;


    @MBeanInfo("The location of the data directory")
    public String getDataDirectory();
    
    /**
     * add by gusj
     * @return return storeEngineAdapter object name
     */
    @MBeanInfo("return storeEngineAdapter object name")
    String getStoreEngineAdapterObjectName() throws Exception;
    
    /**
     * add by gusj
     * @return 
     * @throws Exception
     */
    @MBeanInfo("return connector Name ")
    List<String> getConnectorName()throws Exception;
    
    /**
     * add by gusj
     * @return
     * @throws Exception
     */
    @MBeanInfo("return client object name")
    List<Map<String,String>> getClientObjectNames()throws Exception;

}
