package com.hippo.client;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HippoConnector {
	private static final Logger log = LoggerFactory.getLogger(HippoConnector.class);
	
    private int sessionInstance = ClientConstants.CLIENT_INIT_SESSIONPOOL_SIZE;

    private String brokerUrl;

    private String scheme;

    private String zookeeperUrl;

    private String clusterName;

    public static HippoConnector createConnector(String clusterName, String brokerUrl, String zookeeperUrl) {
        HippoConnector conn = new HippoConnector();
        conn.setClusterName(clusterName);
        conn.setBrokerUrl(brokerUrl);
        conn.setZookeeperUrl(zookeeperUrl);
        return conn;
    }

    public static HippoConnector createConnector(String clusterName, String brokerUrl, String zookeeperUrl, int sessionInstance) {
        HippoConnector conn = new HippoConnector();
        conn.setClusterName(clusterName);
        conn.setBrokerUrl(brokerUrl);
        conn.setZookeeperUrl(zookeeperUrl);
        conn.setSessionInstance(sessionInstance);
        return conn;
    }

    public int getSessionInstance() {
        return sessionInstance;
    }

    public void setSessionInstance(int sessionInstance) {
    	if(sessionInstance > ClientConstants.MAX_POOL_SIZE) {
    		this.sessionInstance = ClientConstants.MAX_POOL_SIZE;
    	} else if(sessionInstance < ClientConstants.MIN_POOL_SIZE) {
    		this.sessionInstance = ClientConstants.MIN_POOL_SIZE;
    	} else {
    		this.sessionInstance = sessionInstance;
    	}
    }

    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setZookeeperUrl(String zookeeperUrl) {
        this.zookeeperUrl = zookeeperUrl;
        if (StringUtils.isEmpty(brokerUrl) && !StringUtils.isEmpty(zookeeperUrl)) {
            scheme = ClientConstants.TRANSPORT_PROTOCOL_CLUSTER;
        }
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        if (!StringUtils.isEmpty(brokerUrl)) {
            try {
                scheme = new URI(brokerUrl).getScheme();
            } catch (URISyntaxException e) {
            	log.error("get scheme happened error. ", e);
            }
        }
    }

    public String getScheme() {
        return scheme;
    }
    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

}
