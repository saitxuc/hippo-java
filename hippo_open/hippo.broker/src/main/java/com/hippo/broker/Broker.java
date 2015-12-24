package com.hippo.broker;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.hippo.broker.plugin.BrokerPlugin;
import com.hippo.broker.transport.TransportConnector;
import com.hippo.broker.useage.SystemUsage;
import com.hippo.client.HippoResult;
import com.hippo.common.Result;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.network.command.Command;
import com.hippo.network.transport.TransportConnectionManager;
/**
 * 
 * @author saitxuc
 *
 */
public interface Broker extends LifeCycle {
	
	/**
	 * 
	 * @param startAsync
	 */
	public void startCacheEngine(boolean startAsync);
	
	/**
	 * 
	 * @param startAsync
	 */
    public void startBroker(boolean startAsync);
    
    /**
     * 
     * @param protocal
     * @param bindPort
     * @return
     * @throws Exception
     */
    public TransportConnector addConnector(String protocal, int bindPort) throws Exception;
	
    /**
     * 
     * @param protocal
     * @param bindPort
     * @param maxconnections
     * @return
     * @throws Exception
     */
    public TransportConnector addConnector(String protocal, int bindPort, int maxconnections) throws Exception;
    
    /**
     * 
     * @param connector
     */
    public boolean removeConnector(TransportConnector connector);
    
    /**
     * 
     * @param command
     * @return
     */
    public HippoResult processCommand(Command command); 
    
    /**
     * 
     * @return
     */
    public String getBrokerName();
    
    /**
     * 
     * @return
     */
    public String getUptime();
    
    /**
     * 
     */
    public void gc();
    
    /**
     * 
     * @return
     */
    public List<TransportConnector> getTransportConnectors();
    
    /**
     * 
     * @param name
     * @return
     */
    public TransportConnector getConnectorByName(String name);
    
    /**
     * 
     * @return
     */
    public File getDataDirectoryFile();
    
    /**
     * 
     * @return
     */
    public SystemUsage getSystemUsage();
    
    /**
     * 
     * @return
     */
    public Map<String, String> getConfigMap();
    
    /**
     * add by gusj
     * @return
     */
    public String getStoreEngineAdapterObjectName() throws Exception;
    
    /**
     * add by gusj
     * @return
     * @throws Exception
     */
    public List<String> getConnectorName()throws Exception;
    
    /**
     * add by gusj
     * @return
     * @throws Exception
     */
    public List<Map<String,String>> getClientObjectNames()throws Exception;
    
    /**
     * 
     * @return
     */
    public TransportConnectionManager createTransportConnectionManager(TransportConnector connector);
    
    /**
     * 
     * @param plugins
     */
    public void setPlugins(BrokerPlugin[] plugins);
    
    /**
     * 
     * @param brokerUris
     */
    void setBrokerUris(String brokerUris);
    
}
