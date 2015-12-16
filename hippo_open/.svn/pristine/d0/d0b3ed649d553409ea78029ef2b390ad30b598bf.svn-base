package com.pinganfu.hippo.client.transport.failover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.client.ClientConstants;
import com.pinganfu.hippo.client.exception.HippoClientException;
import com.pinganfu.hippo.client.transport.simple.SimpleConnectionControl;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.impl.FailoverTransportConnectionFactory;

public class FailoverConnectionControl extends SimpleConnectionControl {

    private static final Logger log = LoggerFactory.getLogger(FailoverConnectionControl.class);

    @Override
    public String getName() {
        return ClientConstants.TRANSPORT_PROTOCOL_FAILOVER;
    }

    @Override
    public Connection createConnection(String userName, String password, String brokerUrl) throws HippoException {
        ConnectionFactory connectionFactory = new FailoverTransportConnectionFactory(brokerUrl);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection(userName, password);
        } catch (Exception e) {
            log.error("create connection happened error in cluster model.", e);
            throw new HippoClientException("create connection happened error in failover model.", 
            		HippoCodeDefine.HIPPO_CONNECTION_FAILURE);
        }
        return connection;
    }
}
