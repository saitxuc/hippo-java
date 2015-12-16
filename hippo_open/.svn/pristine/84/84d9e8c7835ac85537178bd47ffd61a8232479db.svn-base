package com.pinganfu.hippo.replicated;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.pinganfu.hippo.broker.BrokerService;
import com.pinganfu.hippo.broker.PropBrokerService;
import com.pinganfu.hippo.common.config.PropConfigConstants;
import com.pinganfu.hippo.common.util.ResourceUtil;

public class HandlerTest {
    public static void main(String[] args) {
        Properties props;
        try {
            props = load("E:\\hippo\\hippo.properties");
            Map<String, String> configMap = convert(props); 
            BrokerService brokerService = new PropBrokerService(configMap);
            brokerService.init();
            brokerService.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static Properties load(String uri)throws Exception {
        Resource resource = ResourceUtil.resourceFromString(uri);
        Properties props  = PropertiesLoaderUtils.loadProperties(resource);
        return props;
        
    }
    
    private static Map<String, String> convert(Properties props) {
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
        return configMap;
    }
}
