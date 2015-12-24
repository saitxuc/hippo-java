package com.hippo.network.transport.nio;

import com.hippo.common.IdGenerator;
import com.hippo.common.exception.HippoException;
import com.hippo.common.listener.EmptyBaseEventListener;
import com.hippo.common.listener.NettyBaseEventListener;
import com.hippo.network.impl.TransportConnection;
import com.hippo.network.transport.Transport;

/**
 * 
 * @author saitxuc
 * write 2014-7-8
 */
public class NettyConnection extends TransportConnection {

	public NettyConnection(Transport transport,
			IdGenerator clientIdGenerator, IdGenerator connectionIdGenerator)
			throws Exception {
		super(transport, clientIdGenerator, connectionIdGenerator);
	}
	
	@Override
	protected void instanceEventListener() {
    	this.addEventListener(new NettyEventListener(this));
    }
	
    private class NettyEventListener extends NettyBaseEventListener {

        private TransportConnection transportConnection = null;

        public NettyEventListener(TransportConnection transportConnection) {
            this.transportConnection = transportConnection;
        }

        @Override
        protected void onReconnect() throws HippoException{
            super.onReconnect();
            try {
            	NettyConnection.this.ensureConnectionInfoSent();
            } catch (HippoException e) {
                LOG.error(" fire register channel, but send connectionInfo to server happen error! ", e);
                throw new HippoException(e.getMessage(), e.getErrorCode());
            }
        }
    }
}




