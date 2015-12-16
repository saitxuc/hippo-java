package com.pinganfu.hippo.network.transport;

import java.net.URI;

/**
 * 
 * @author dongpj
 */
public interface CompositeTransport extends Transport {
    void add(boolean rebalance, URI[] uris);

    void remove(boolean rebalance, URI[] uris);
}
