package com.hippo.network.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.network.TransportFactory;
import com.hippo.network.transport.Transport;
import com.hippo.network.transport.TransportFactoryFinder;

/**
 * @author DPJ
 */
public class FailoverTransportConnectionFactory extends TransportConnectionFactory {

    protected static final Logger LOG = LoggerFactory.getLogger(FailoverTransportConnectionFactory.class);

    public FailoverTransportConnectionFactory(String brokerURL) {
        super(brokerURL);
    }
 
    @Override
    public Transport createTransport(String schema) {
        synchronized (lock) {
            TransportFactory transportFactory = TransportFactoryFinder.getTransportFactory(schema);
            try {
                final Transport transport = transportFactory.connect(brokerURL, commandManager);
                return transport;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return null;
            }
        }

    }
}
