package com.hippo.broker;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hippo.common.config.PropConfigConstants;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.cluster.controltable.CtrlTableClusterBrokerService;
import com.hippo.broker.cluster.simple.MsClusterBrokerService;
import com.hippo.broker.transport.HippoBrokerCommandManager;
import com.hippo.broker.transport.TransportConnector;
import com.hippo.broker.useage.SystemUsage;
import com.hippo.client.HippoResult;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.serializer.SerializerFactory;
import com.hippo.common.util.ClassUtil;
import com.hippo.common.util.LimitUtils;
import com.hippo.common.util.NetUtils;
import com.hippo.common.util.URISupport;
import com.hippo.network.CommandManager;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;
import com.hippo.store.StoreEngine;
import com.hippo.store.StoreEngineFactory;

/**
 * 
 * @author saitxuc
 *
 */
public class PropBrokerService extends BrokerService {

    private static final Logger LOG = LoggerFactory.getLogger(PropBrokerService.class);

    public static final String DEFAULT_NIO_TYPE = "netty";

    private Map<String, String> configMap = new HashMap<String, String>();

    private BrokerService internalBroker = null;

    public PropBrokerService(Map<String, String> configMap) {
        this.configMap = configMap;
        populate();

    }

    private void populate() {
        String brokerType = this.configMap.get(PropConfigConstants.BROKER_TYPE);
        String replicatedPort = this.configMap.get(PropConfigConstants.REPLICATED_PORT);
        LOG.info("brokerType get from the config map -> " + brokerType);
        if (StringUtils.isEmpty(brokerType) || PropConfigConstants.DEFAULT_BROKER_TYPE.equals(brokerType)) {
            LOG.info("internalBroker is the default service");
            internalBroker = new BrokerService();
            internalBroker.setSimpleMode(true);
            preLoadInternalBrokerService(configMap);
        } else {
            String zkUrl = this.configMap.get(PropConfigConstants.ZK_URL);
            if (StringUtils.isEmpty(zkUrl)) {
                throw new RuntimeException("  The mode of cluster , but zkUrl : " + zkUrl + " is ilLegal. please check your config. ");
            }
            String clusterName = this.configMap.get(PropConfigConstants.CLUSTER_NAME);
            if (StringUtils.isEmpty(clusterName)) {
                throw new RuntimeException("  The mode of cluster , but clusterName : " + clusterName + " is ilLegal. please check your config. ");
            }
            if (PropConfigConstants.MASTERSLAVE_BROKER_TYPE.equals(brokerType)) {
                LOG.info("internalBroker is the MsClusterBrokerService");
                internalBroker = new MsClusterBrokerService(zkUrl, clusterName, replicatedPort);
                preLoadInternalBrokerService(configMap);
            }

            if (PropConfigConstants.CLUSTER_BROKER_TYPE.equals(brokerType)) {
                internalBroker = new CtrlTableClusterBrokerService(zkUrl, clusterName, replicatedPort, this.configMap);
                preLoadCommonProp();
                
            }
        }

        if (internalBroker == null) {
            throw new RuntimeException("  brokerType : " + brokerType + " is ilLegal. please check your config. ");
        }

    }
    
    private void preLoadCommonProp() {
    	if(internalBroker == null) {
    		throw new RuntimeException(" internalBroker do not init, internalBroker is null. ");
    	}
    	
    	String nioType = this.configMap.get(PropConfigConstants.NIO_TYPE);
        internalBroker.setNioType(nioType);
        String bname = this.configMap.get(PropConfigConstants.BROKER_NAME);
        if (!StringUtils.isEmpty(bname)) {
            this.internalBroker.setBrokerName(bname);
        }
        String usejmxs = this.configMap.get(PropConfigConstants.BROKER_USEJMX);
        if ("true".equalsIgnoreCase(usejmxs)) {
            this.internalBroker.useJmx = true;
        }
        String jmxconnectorHost = this.configMap.get(PropConfigConstants.JMX_CONNECTOR_HOST);
        if (!StringUtils.isEmpty(jmxconnectorHost)) {
        	this.internalBroker.jmxConnectorHost = jmxconnectorHost;
        }
        String serializerType = this.configMap.get(PropConfigConstants.BROKER_SERIALIZER);
        if (!StringUtils.isEmpty(serializerType)) {
            this.internalBroker.serializer = SerializerFactory.findSerializer(serializerType);
        }
        
        String commandManagerClass = this.configMap.get(PropConfigConstants.BROKER_COMMANDMANAGER_CLASS);
        CommandManager commandManager = null;
        if(StringUtils.isNotBlank(commandManagerClass)) {
        	Class<?> commandManagerClazz = ClassUtil.classByClassName(commandManagerClass);
        	Class<?>[] types = new Class<?>[1];
        	types[0] = CommandHandle.class;
        	Object[] args = new Object[1];
        	args[0] = internalBroker;
        	commandManager = (CommandManager)ClassUtil.intanceByClass(commandManagerClazz, types, args);
        }
        if(commandManager == null) {
        	commandManager = new HippoBrokerCommandManager(internalBroker);
        }
        this.internalBroker.setCommandManager(commandManager);
        
        this.brokerUris = this.configMap.get(PropConfigConstants.BROKER_URIS);
        if (!StringUtils.isEmpty(brokerUris)) {
        	if(brokerUris.indexOf("localhost") != -1) {
            	brokerUris = brokerUris.replace("localhost", NetUtils.getLocalHost());
            }
            if(brokerUris.indexOf("127.0.0.1") != -1) {
            	brokerUris = brokerUris.replace("127.0.0.1", NetUtils.getLocalHost());
            }
            internalBroker.setBrokerUris(brokerUris);
        	String[] brokerUriArray = StringUtils.split(brokerUris, ";");
            for (String brokerUri : brokerUriArray) {
                instanceConnector(brokerUri);
            }
        }
    }
    
    private void preLoadInternalBrokerService(Map<String, String> configMap) {
    	preLoadCommonProp();
       String dbType = this.configMap.get(PropConfigConstants.BROKER_STORE);
        if (!StringUtils.isEmpty(dbType)) {
            StoreEngine storeEngine = StoreEngineFactory.findStoreEngine(dbType);
            Cache cache = new DefaultCache(this.internalBroker, storeEngine);
            String buckets = this.configMap.get(PropConfigConstants.DB_BUCKETS);
            if (!StringUtils.isEmpty(buckets)) {
                String[] barray = StringUtils.split(buckets, ",");
                List<BucketInfo> blist = new ArrayList<BucketInfo>();
                for (String bucket : barray) {
                    BucketInfo materInfo = new BucketInfo(Integer.parseInt(bucket), false);
                    blist.add(materInfo);
                    BucketInfo slaveInfo = new BucketInfo(Integer.parseInt(bucket), true);
                    blist.add(slaveInfo);
                }
                cache.setBuckets(blist);
            }

            cache.setInitParams(configMap);
            this.internalBroker.setCache(cache);
        }
        
        String capacityLimit = this.configMap.get(PropConfigConstants.DB_LIMIT);
        if (!StringUtils.isEmpty(capacityLimit)) {
            long capacityLimitNum = LimitUtils.calculationLimit(capacityLimit);
            if (capacityLimitNum == -1) {
                throw new RuntimeException(" db limit set Illegal, please check! ");
            }
            this.internalBroker.setLimit(capacityLimitNum);
        }
    }
    
    private void instanceConnector(String uri) {
        Map<String, String> qmap = new HashMap<String, String>();;
        try {
            if (uri.lastIndexOf('?') != -1) {
                qmap = URISupport.parseQuery(uri);
                uri = uri.substring(0, uri.lastIndexOf('?'));
            }
            String maxconnectionstr = qmap.get("maxconnections");
            URI bUri = createURI(uri);
            int bindPort = bUri.getPort();
            String sheme = bUri.getScheme();
            if (!StringUtils.isEmpty(maxconnectionstr)) {
                int maxconnections = Integer.parseInt(maxconnectionstr);
                TransportConnector connector = this.addConnector(sheme, bindPort, maxconnections);
                connector.setName(bUri.getScheme());
            } else {
                TransportConnector connector = this.addConnector(sheme, bindPort);
                connector.setName(bUri.getScheme());
            }
        } catch (Exception e) {
            LOG.error(" prop instance connector for uri : " + uri, e);
        }
    }

    @Override
    public void startCacheEngine(boolean startAsync) {
        internalBroker.startCacheEngine(startAsync);
    }

    @Override
    public void startBroker(boolean startAsync) {
        internalBroker.startBroker(startAsync);
    }

    @Override
    public TransportConnector addConnector(String protocal, int bindPort) throws Exception {
        return internalBroker.addConnector(protocal, bindPort);
    }

    @Override
    public TransportConnector addConnector(String protocal, int bindPort, int maxconnections) throws Exception {
        return internalBroker.addConnector(protocal, bindPort, maxconnections);
    }

    @Override
    public boolean removeConnector(TransportConnector connector) {
        return internalBroker.removeConnector(connector);
    }

    @Override
    public HippoResult processCommand(Command command) {
        return internalBroker.processCommand(command);
    }

    @Override
    public String getBrokerName() {
        return internalBroker.getBrokerName();
    }

    @Override
    public String getUptime() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void gc() {
        internalBroker.gc();
    }

    @Override
    public List<TransportConnector> getTransportConnectors() {
        return internalBroker.getTransportConnectors();
    }

    @Override
    public TransportConnector getConnectorByName(String name) {
        return internalBroker.getConnectorByName(name);
    }

    @Override
    public File getDataDirectoryFile() {
        return internalBroker.getDataDirectoryFile();
    }

    @Override
    public SystemUsage getSystemUsage() {
        return internalBroker.getSystemUsage();
    }

    @Override
    public Map<String, String> getConfigMap() {
        return internalBroker.getConfigMap();
    }

    @Override
    public void doInit() {
        internalBroker.doInit();
    }

    @Override
    public void doStart() {
        internalBroker.doStart();
    }

    @Override
    public void doStop() {
        internalBroker.doStop();
    }

    private static URI createURI(String brokerURL) {
        try {
            return new URI(brokerURL);
        } catch (URISyntaxException e) {
            throw (IllegalArgumentException) new IllegalArgumentException("Invalid broker URI: " + brokerURL).initCause(e);
        }
    }
     
    
}
