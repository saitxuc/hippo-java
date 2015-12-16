package com.pinganfu.hippo.bootstrap.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.pinganfu.hippo.broker.BrokerFactory;
import com.pinganfu.hippo.broker.BrokerService;
/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public class StartAdjure extends AbstractAdjure {
	
	public static final String DEFAULT_CONFIG_URI = "prop:hippo-single.properties";
	
	protected String[] helpFile = new String[] {
	        "Task Usage: Main start [start-options] [uri]",
	        "Description: Creates and starts a broker using a configuration file, or a broker URI.",
	        "",
	        "Start Options:",
	        "    -D<name>=<value>      Define a system property.",
	        "    --version             Display the version information.", 
	        "    -h,-?,--help          Display the start broker help information.",
	        "",
	        "URI:",
	        "",
	        "    XBean based broker configuration:",
	        "",
	        "        Example: Main xbean:file:hippo.xml/hippo.properies ",
	        "            Loads the xbean configuration file from the current working directory",
	        "        Example: Main xbean:hippo.xml/hippo.properies",
	        "            Loads the xbean configuration file from the classpath",
	        "",
	        "    URI Parameter based broker configuration:",
	        ""
	    };
	
	private URI configURI;
    private List<BrokerService> brokers = new ArrayList<BrokerService>(5);
    
    @Override
    public String getName() {
        return "start";
    }

    @Override
    public String getOneLineDescription() {
        return "Creates and starts a broker using a configuration file, or a broker URI.";
    }
    
    protected void runTask(List<String> brokerURIs) throws Exception {
        try {
            // If no config uri, use default setting
            if (brokerURIs.isEmpty()) {
                setConfigUri(new URI(DEFAULT_CONFIG_URI));
                startBroker(getConfigUri());

                // Set configuration data, if available, which in this case
                // would be the config URI
            } else {
                String strConfigURI;

                while (!brokerURIs.isEmpty()) {
                    strConfigURI = (String)brokerURIs.remove(0);

                    try {
                        setConfigUri(new URI(strConfigURI));
                    } catch (URISyntaxException e) {
                        context.printException(e);
                        return;
                    }

                    startBroker(getConfigUri());
                }
            }

            // Prevent the main thread from exiting unless it is terminated
            // elsewhere
        } catch (Exception e) {
            context.printException(new RuntimeException("Failed to execute start task. Reason: " + e, e));
            throw new Exception(e);
        }
        
        // The broker start up fine.  If this unblocks it's cause they were stopped
        // and this would occur because of an internal error (like the DB going offline)
        waitForShutdown();
    }
    
    public void startBroker(URI configURI) throws Exception {
        System.out.println("Loading hippo broker from: " + configURI);
        BrokerService broker = BrokerFactory.createBroker(configURI);
        brokers.add(broker);
        broker.start();
        if (!broker.waitUntilStarted()) {
            throw new Exception(broker.getStartException());
        }
    }
    
    protected void waitForShutdown() throws Exception {
        final boolean[] shutdown = new boolean[] {
            false
        };
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (Iterator<BrokerService> i = brokers.iterator(); i.hasNext();) {
                    try {
                        BrokerService broker = i.next();
                        broker.stop();
                    } catch (Exception e) {
                    }
                }
            }
        });
        
        final AtomicInteger brokerCounter = new AtomicInteger(brokers.size());
        for (BrokerService bs : brokers) {
            bs.addShutdownHook(new Runnable() {
                public void run() {
                    // When the last broker lets us know he is closed....
                    if( brokerCounter.decrementAndGet() == 0 ) {
                        synchronized (shutdown) {
                            shutdown[0] = true;
                            shutdown.notify();
                        }
                    }
                }
            });
        }

        // Wait for any shutdown event
        synchronized (shutdown) {
            while (!shutdown[0]) {
                try {
                    shutdown.wait();
                } catch (InterruptedException e) {
                }
            }
        }

    }
    
    public void setConfigUri(URI uri) {
        configURI = uri;
    }
    
    public URI getConfigUri() {
        return configURI;
    }
    
    protected void printHelp() {
        context.printHelp(helpFile);
    }
    
}
