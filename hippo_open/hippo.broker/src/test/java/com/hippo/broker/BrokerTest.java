package com.hippo.broker;

import java.util.ArrayList;
import java.util.List;

import com.hippo.broker.plugin.BrokerPlugin;
import com.hippo.broker.security.AuthorizationPlugin;
import com.hippo.broker.transport.TransportConnector;
import com.hippo.common.domain.BucketInfo;

/**
 * 
 * @author saitxuc
 * write 2014-8-11
 */
public class BrokerTest {
	
	public static void main(final String[] args) throws Exception {
		
		//System.out.println("----------->>"+System.getProperty("-XX:MaxDirectMemorySize"));
		BrokerService simpleBroker = new BrokerService();
		simpleBroker.setBrokerName("Hippo-broker");
		simpleBroker.setUseJmx(true);
		simpleBroker.setNioType("netty");
		simpleBroker.setSimpleMode(true);
        
        List<BucketInfo> buckets = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, false);
        buckets.add(info);
        
        simpleBroker.setBuckets(buckets);
        simpleBroker.setLimit(10000000000L);
        BrokerPlugin authenticationPlugin = new AuthorizationPlugin();
		simpleBroker.setPlugins(new BrokerPlugin[] {authenticationPlugin});
		simpleBroker.init();
		
		TransportConnector connector = simpleBroker.addConnector("hippo", 61300,  1);
		connector.setName("hippo");
		
		
		simpleBroker.start();
		//simpleBroker.stop();
		
		
	}
	
}
