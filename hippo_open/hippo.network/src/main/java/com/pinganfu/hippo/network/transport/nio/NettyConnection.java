package com.pinganfu.hippo.network.transport.nio;

import com.pinganfu.hippo.common.IdGenerator;
import com.pinganfu.hippo.network.impl.TransportConnection;
import com.pinganfu.hippo.network.transport.Transport;

/**
 * 
 * @author saitxuc
 * write 2014-7-8
 */
public class NettyConnection extends TransportConnection {

	protected NettyConnection(Transport transport,
			IdGenerator clientIdGenerator, IdGenerator connectionIdGenerator)
			throws Exception {
		super(transport, clientIdGenerator, connectionIdGenerator);
	}
	
}




