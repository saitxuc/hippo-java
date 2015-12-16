package com.pinganfu.hippo.network.transport.failover;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.pinganfu.hippo.common.Extension;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.util.IntrospectionSupport;
import com.pinganfu.hippo.common.util.URISupport;
import com.pinganfu.hippo.common.util.URISupport.CompositeData;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportServer;
import com.pinganfu.hippo.network.transport.failover.client.FailoverTransport;

/**
 * 
 * @author DPJ
 */
@Extension("netty-failover")
public class FailoverTransportFactory implements TransportFactory {

    @Override
    public TransportServer bind(int port, CommandManager commandManager) throws IOException {
        throw new IOException("Invalid server URI: " + port);
    }

    @Override
    public String getName() {
        return "netty-failover";
    }

    @Override
    public Transport connect(URI uri, CommandManager commandManager) throws Exception {
        try {
            Transport transport = createTransport(URISupport.parseComposite(uri));
            // TODO: check
            // transport = new MutexTransport(transport);
            // transport = new ResponseCorrelator(transport);
            return transport;
        } catch (URISyntaxException e) {
            throw new IOException("Invalid location: " + uri);
        }
    }

    @Override
    public Transport connect(String host, int port, CommandManager commandManager) throws Exception {
        throw new HippoException("connect(host, port, commandManager) method not supported");
    }

    /**
     * @param location
     * @return
     * @throws IOException
     */
    public Transport createTransport(CompositeData compositData) throws IOException {
        Map<String, String> options = compositData.getParameters();
        FailoverTransport transport = createTransport(options);
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid connect parameters: " + options);
        }
        transport.add(false, compositData.getComponents());
        return transport;
    }

    public FailoverTransport createTransport(Map<String, String> parameters) throws IOException {
        FailoverTransport transport = new FailoverTransport();
        IntrospectionSupport.setProperties(transport, parameters);
        return transport;
    }

}
