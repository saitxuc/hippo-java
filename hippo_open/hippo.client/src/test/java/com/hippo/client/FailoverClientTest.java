package com.hippo.client;

import com.hippo.client.impl.HippoClientImpl;

public class FailoverClientTest {

	public static void main(String[] args) {
		HippoConnector hippoConnector = new HippoConnector();
        hippoConnector.setBrokerUrl("failover:(hippo://10.59.2.112:61000,hippo://192.168.1.187:61000)");
        hippoConnector.setSessionInstance(5);
        try{
        	final HippoClientImpl client = new HippoClientImpl(hippoConnector);
            client.start();
            client.set("test", "tt");
            
            System.out.println("------client---------"+client.get("test").isSuccess());
        }catch(Exception e) {
        	
        }
        
        
	}

}
