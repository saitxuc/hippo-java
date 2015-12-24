package com.hippo.jmx;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.Broker;
import com.hippo.broker.transport.TransportConnector;

/***
 * 
 * @author saitxuc
 * write 2014-8-12
 */
public class BrokerView implements BrokerViewMBean {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerView.class);
    private final Broker brokerService;
    
    public BrokerView(Broker brokerService) throws Exception {
        this.brokerService = brokerService;
    }

    public String getBrokerName() {
        return safeGetBroker().getBrokerName();
    }

    public String getBrokerVersion() {
        return "1.0.0";
    }

    @Override
    public String getUptime() {
        return brokerService.getUptime();
    }

    public void gc() throws Exception {
        brokerService.gc();
    }

    public void start() {
        brokerService.start();
    }

    public void stop(){
        brokerService.stop();
    }

    public int getMemoryPercentUsage() {
        return brokerService.getSystemUsage().getMemoryUsage().getPercentUsage();
    }

    public long getMemoryLimit() {
        return brokerService.getSystemUsage().getMemoryUsage().getLimit();
    }

    public void setMemoryLimit(long limit) {
        brokerService.getSystemUsage().getMemoryUsage().setLimit(limit);
    }

    public long getStoreLimit() {
        return brokerService.getSystemUsage().getStoreUsage().getLimit();
    }

    public int getStorePercentUsage() {
        return brokerService.getSystemUsage().getStoreUsage().getPercentUsage();
    }

    public void setStoreLimit(long limit) {
        brokerService.getSystemUsage().getStoreUsage().setLimit(limit);
    }

    public void terminateJVM(int exitCode) {
        System.exit(exitCode);
    }

    public String addConnector(String protocal, int bindPort) throws Exception {
        TransportConnector connector = brokerService.addConnector(protocal,bindPort );
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + (protocal + "://" + bindPort));
        }
        connector.start();
        return connector.getName();
    }

    public boolean removeConnector(String connectorName) throws Exception {
        TransportConnector connector = brokerService.getConnectorByName(connectorName);
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + connectorName);
        }
        connector.stop();
        return brokerService.removeConnector(connector);
    }

    public void reloadLog4jProperties() throws Throwable {

        // Avoid a direct dependency on log4j.. use reflection.
        try {
            ClassLoader cl = getClass().getClassLoader();
            Class<?> logManagerClass = cl.loadClass("org.apache.log4j.LogManager");

            Method resetConfiguration = logManagerClass.getMethod("resetConfiguration", new Class[]{});
            resetConfiguration.invoke(null, new Object[]{});

            String configurationOptionStr = System.getProperty("log4j.configuration");
            URL log4jprops = null;
            if (configurationOptionStr != null) {
                try {
                    log4jprops = new URL(configurationOptionStr);
                } catch (MalformedURLException ex) {
                    log4jprops = cl.getResource("log4j.properties");
                }
            } else {
               log4jprops = cl.getResource("log4j.properties");
            }

            if (log4jprops != null) {
                Class<?> propertyConfiguratorClass = cl.loadClass("org.apache.log4j.PropertyConfigurator");
                Method configure = propertyConfiguratorClass.getMethod("configure", new Class[]{URL.class});
                configure.invoke(null, new Object[]{log4jprops});
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public String getDataDirectory() {
        File file = brokerService.getDataDirectoryFile();
        try {
            return file != null ? file.getCanonicalPath():"";
        } catch (IOException e) {
            return "";
        }
    }

    private Broker safeGetBroker() {
        if (brokerService == null) {
            throw new IllegalStateException("Broker is not yet started.");
        }

        return brokerService;
    }

	@Override
	public void init() {
		
	}

	@Override
	public boolean isStarted() {
		return brokerService.isStarted();
	}

	@Override
	public Throwable getStartException() {
		// TODO Auto-generated method stub
		return brokerService.getStartException();
	}
	
	@Override
    public String getStoreEngineAdapterObjectName() throws Exception{
    	return brokerService.getStoreEngineAdapterObjectName();
    }
    
    @Override
    public List<String> getConnectorName() throws Exception {
    	return brokerService.getConnectorName();
    }
    
    @Override
    public List<Map<String,String>> getClientObjectNames() throws Exception {
    	return brokerService.getClientObjectNames();
    }
}
