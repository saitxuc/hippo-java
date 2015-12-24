package com.hippo.client;

import com.hippo.client.impl.HippoClientImpl;

public class DefaultHippoClient extends HippoClientImpl {

    public static DefaultHippoClient createClient(HippoConnector connector) {
        DefaultHippoClient client = new DefaultHippoClient();
        client.setConnector(connector);
        return client;
    }
}
