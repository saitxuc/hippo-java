package com.hippo.broker;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.hippo.broker.plugin.BrokerPlugin;
import com.hippo.broker.transport.TransportConnector;
import com.hippo.broker.useage.SystemUsage;
import com.hippo.client.HippoResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.TransportConnectionManager;

/**
 * 
 * @author saitxuc
 * 2015-4-27
 */
public class BrokerFilter  implements Broker {
	
	protected final Broker next;
	
	public BrokerFilter(Broker next) {
		this.next = next;
	}

	@Override
	public boolean isStarted() {
		return next.isStarted();
	}

	@Override
	public Throwable getStartException() {
		return next.getStartException();
	}

	@Override
	public void startCacheEngine(boolean startAsync) {
		next.startCacheEngine(startAsync);
	}

	@Override
	public void startBroker(boolean startAsync) {
		next.startBroker(startAsync);
	}

	@Override
	public TransportConnector addConnector(String protocal, int bindPort)
			throws Exception {
		return next.addConnector(protocal, bindPort);
	}

	@Override
	public TransportConnector addConnector(String protocal, int bindPort,
			int maxconnections) throws Exception {
		return next.addConnector(protocal, bindPort, maxconnections);
	}

	@Override
	public boolean removeConnector(TransportConnector connector) {
		return next.removeConnector(connector);
	}

	@Override
	public HippoResult processCommand(Command command) {
		return next.processCommand(command);
	}

	@Override
	public String getBrokerName() {
		return next.getBrokerName();
	}

	@Override
	public String getUptime() {
		return next.getUptime();
	}

	@Override
	public void gc() {
		next.gc();
	}

	@Override
	public List<TransportConnector> getTransportConnectors() {
		return next.getTransportConnectors();
	}

	@Override
	public TransportConnector getConnectorByName(String name) {
		return next.getConnectorByName(name);
	}

	@Override
	public File getDataDirectoryFile() {
		return next.getDataDirectoryFile();
	}

	@Override
	public SystemUsage getSystemUsage() {
		return next.getSystemUsage();
	}

	@Override
	public Map<String, String> getConfigMap() {
		return next.getConfigMap();
	}

	@Override
	public String getStoreEngineAdapterObjectName() throws Exception {
		return next.getStoreEngineAdapterObjectName();
	}

	@Override
	public List<String> getConnectorName() throws Exception {
		return next.getConnectorName();
	}

	@Override
	public List<Map<String, String>> getClientObjectNames() throws Exception {
		return next.getClientObjectNames();
	}

	@Override
	public void init() {
		next.init();
	}

	@Override
	public void start() {
		next.start();
	}

	@Override
	public void stop() {
		next.stop();
	}

	@Override
	public TransportConnectionManager createTransportConnectionManager(TransportConnector connector) {
		return next.createTransportConnectionManager(connector);
	}

	@Override
	public void setPlugins(BrokerPlugin[] plugins) {
		next.setPlugins(plugins);
	}

	@Override
	public void setBrokerUris(String brokerUris) {
		next.setBrokerUris(brokerUris);
	}

}
