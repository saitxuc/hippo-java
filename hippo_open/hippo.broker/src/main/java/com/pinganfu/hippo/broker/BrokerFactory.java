package com.pinganfu.hippo.broker;

import java.io.IOException;
import java.net.URI;

import com.pinganfu.hippo.common.util.FactoryFinder;
import com.pinganfu.hippo.common.util.IOExceptionSupport;

/**
 * 
 * @author saitxuc
 * 
 */
public class BrokerFactory {

	private static final FactoryFinder BROKER_FACTORY_HANDLER_FINDER = new FactoryFinder(
			"META-INF/services/com/pinganfu/hippo/broker/");

	private BrokerFactory() {
	}

	public static BrokerFactoryHandler createBrokerFactoryHandler(String type)
			throws IOException {
		try {
			return (BrokerFactoryHandler) BROKER_FACTORY_HANDLER_FINDER
					.newInstance(type);
		} catch (Throwable e) {
			throw IOExceptionSupport.create("Could not load " + type
					+ " factory:" + e, e);
		}
	}

	public static BrokerService createBroker(URI brokerURI) throws Exception {
		return createBroker(brokerURI, false);
	}

	/**
	 * 
	 * @param brokerURI
	 * @param startBroker
	 * @return
	 * @throws Exception
	 */
	public static BrokerService createBroker(URI brokerURI, boolean startBroker)
			throws Exception {
		if (brokerURI.getScheme() == null) {
			throw new IllegalArgumentException(
					"Invalid broker URI, no scheme specified: " + brokerURI);
		}
		BrokerFactoryHandler handler = createBrokerFactoryHandler(brokerURI
				.getScheme());
		BrokerService broker = handler.createBroker(brokerURI);
		if (startBroker) {
			broker.start();
		}
		return broker;
	}

	/**
	 * 
	 * @param brokerURI
	 * @return
	 * @throws Exception
	 */
	public static BrokerService createBroker(String brokerURI) throws Exception {
		return createBroker(new URI(brokerURI));
	}

	/**
	 * 
	 * @param brokerURI
	 * @param startBroker
	 * @return
	 * @throws Exception
	 */
	public static BrokerService createBroker(String brokerURI,
			boolean startBroker) throws Exception {
		return createBroker(new URI(brokerURI), startBroker);
	}

	private static final ThreadLocal<Boolean> START_DEFAULT = new ThreadLocal<Boolean>();

	public static void setStartDefault(boolean startDefault) {
		START_DEFAULT.set(startDefault);
	}

	public static void resetStartDefault() {
		START_DEFAULT.remove();
	}

	public static boolean getStartDefault() {
		Boolean value = START_DEFAULT.get();
		if (value == null) {
			return true;
		}
		return value.booleanValue();
	}

}
