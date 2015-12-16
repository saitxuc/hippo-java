package com.pinganfu.hippo.client.transport.netty.server;

import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.HessionSerializer;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.ServerFortressFactory;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.impl.TransportServerFortressFactory;
import com.pinganfu.hippo.network.transport.TransportServer;
import com.pinganfu.hippo.network.transport.nio.RecoveryTransportFactory;
import com.pinganfu.hippo.network.transport.nio.NettyTransportFactory;

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
