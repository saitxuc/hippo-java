package com.hippo.client.transport.netty.server;

import com.hippo.common.serializer.Serializer;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.HessionSerializer;
import com.hippo.network.ServerFortress;
import com.hippo.network.ServerFortressFactory;
import com.hippo.network.TransportFactory;
import com.hippo.network.impl.TransportServerFortressFactory;
import com.hippo.network.transport.TransportServer;
import com.hippo.network.transport.nio.RecoveryTransportFactory;
import com.hippo.network.transport.nio.NettyTransportFactory;

/**
 * 
 * @author saitxuc
 *  write 2014-7-2
 *
 */
public class HippoServerTest {
	
	public static void main(final String[] args) throws Exception {
		/***
		TransportFactory transportFactory = new FailoverTransportFactory();
		TransportServer server = transportFactory.bind(61300, new EchoCommandManager());
		server.start();
		***/
		ServerFortressFactory sfactory = new TransportServerFortressFactory("tcp", 61300, new EchoCommandManager(), null);
		ServerFortress serverFortress = sfactory.createServer();
		
		serverFortress.start();
		
		//server.stop();
		
	}
	
}
