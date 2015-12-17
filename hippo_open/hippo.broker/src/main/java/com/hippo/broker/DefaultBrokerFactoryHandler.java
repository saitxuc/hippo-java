package com.hippo.broker;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.hippo.common.config.PropConfigConstants;
import com.hippo.common.util.IntrospectionSupport;
import com.hippo.common.util.ResourceUtil;
import com.hippo.common.util.URISupport;

/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public class DefaultBrokerFactoryHandler implements BrokerFactoryHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBrokerFactoryHandler.class);

    public BrokerService createBroker(URI configuri) throws Exception {
        String uri = configuri.getSchemeSpecificPart();
        if (uri.lastIndexOf('?') != -1) {
            IntrospectionSupport.setProperties(this, URISupport.parseQuery(uri));
            uri = uri.substring(0, uri.lastIndexOf('?'));
        }
        LOG.info("properties load uri is " + uri);
        Properties props = load(uri);
        Map<String, String> configMap = convert(props);
        if (configMap != null) {
            for (Entry<String, String> entry : configMap.entrySet()) {
                LOG.info("hippo configMap setting:" + entry.getKey() + "-->" + entry.getValue());
            }
        }
        BrokerService brokerService = new PropBrokerService(configMap);
        return brokerService;
    }

    private Properties load(String uri) throws Exception {
        Resource resource = ResourceUtil.resourceFromString(uri);
        Properties props = PropertiesLoaderUtils.loadProperties(resource);
        return props;
    }

    private Map<String, String> convert(Properties props) {
        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put(PropConfigConstants.BROKER_NAME, props.getProperty(PropConfigConstants.BROKER_NAME));
        configMap.put(PropConfigConstants.BROKER_USEJMX, props.getProperty(PropConfigConstants.BROKER_USEJMX));
        configMap.put(PropConfigConstants.BROKER_SERIALIZER, props.getProperty(PropConfigConstants.BROKER_SERIALIZER));
        configMap.put(PropConfigConstants.BROKER_URIS, props.getProperty(PropConfigConstants.BROKER_URIS));
        configMap.put(PropConfigConstants.BROKER_STORE, props.getProperty(PropConfigConstants.BROKER_STORE));
        configMap.put(PropConfigConstants.DB_BUCKETS, props.getProperty(PropConfigConstants.DB_BUCKETS));
        configMap.put(PropConfigConstants.DB_LIMIT, props.getProperty(PropConfigConstants.DB_LIMIT));
        configMap.put(PropConfigConstants.BROKER_TYPE, props.getProperty(PropConfigConstants.BROKER_TYPE));
        configMap.put(PropConfigConstants.CLUSTER_NAME, props.getProperty(PropConfigConstants.CLUSTER_NAME));
        configMap.put(PropConfigConstants.ZK_URL, props.getProperty(PropConfigConstants.ZK_URL));
        configMap.put(PropConfigConstants.REPLICATED_PORT, props.getProperty(PropConfigConstants.REPLICATED_PORT));
        configMap.put(PropConfigConstants.STORE_EXPIRE_COUNT_LIMIT, props.getProperty(PropConfigConstants.STORE_EXPIRE_COUNT_LIMIT));
        configMap.put(PropConfigConstants.STORE_LRU_FATE, props.getProperty(PropConfigConstants.STORE_LRU_FATE));
        configMap.put(PropConfigConstants.STORE_DATA_SIZE_TYPE, props.getProperty(PropConfigConstants.STORE_DATA_SIZE_TYPE));
        configMap.put(PropConfigConstants.JMX_CONNECTOR_HOST, props.getProperty(PropConfigConstants.JMX_CONNECTOR_HOST));
        configMap.put(PropConfigConstants.LEVELDB_MDB_USE_FLAG, props.getProperty(PropConfigConstants.LEVELDB_MDB_USE_FLAG));
        configMap.put(PropConfigConstants.LEVELDB_MDB_USE_LIMIT, props.getProperty(PropConfigConstants.LEVELDB_MDB_USE_LIMIT));
        configMap.put(PropConfigConstants.STORE_BIT_SIZE_TYPE, props.getProperty(PropConfigConstants.STORE_BIT_SIZE_TYPE));
        return configMap;
    }
}
