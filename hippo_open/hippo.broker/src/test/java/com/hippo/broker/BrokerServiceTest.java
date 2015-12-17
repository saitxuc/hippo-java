package com.hippo.broker;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.hippo.client.command.GetCommand;
import com.hippo.client.command.SetCommand;
import com.hippo.common.exception.HippoException;
import com.hippo.network.Connection;
import com.hippo.network.ConnectionFactory;
import com.hippo.network.impl.TransportConnectionFactory;

public class BrokerServiceTest {
	private BrokerService brokerService;
	Connection conn;

	@BeforeMethod
	public void setUp() {
		try {
			brokerService = BrokerFactory.createBroker(new URI("prop:conf/test-hippo-single.properties"));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		brokerService.setCache(new DbCacheService(brokerService, StoreEngineFactory.findStoreEngine(StoreConstants.STORE_ENGINE_LEVELDB)));
		brokerService.init();
		brokerService.start();

		ConnectionFactory factory = new TransportConnectionFactory(null, null,  "tcp://" + "127.0.0.1" + ":" + 61000);

		try {
			conn = factory.createConnection();
		} catch (HippoException e) {
			e.printStackTrace();
		}
		conn.init();
		conn.start();
	}

	@Test
	public void test_putData() {
		SetCommand command = new SetCommand();
		command.setData("11".getBytes());
		try {
			conn.asyncSendPacket(command);
		} catch (HippoException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_getData() {
		GetCommand command = new GetCommand();
		command.setData(("key" + 101%5).getBytes());
		try {
			Object object = conn.syncSendPacket(command, 60000);
			Assert.assertNotNull(object);
		} catch (HippoException e) {
			e.printStackTrace();
		}
	}
}
