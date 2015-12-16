package com.pinganfu.hippo.client.listener;

import com.pinganfu.hippo.client.transport.AbstractClientConnectionControl;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.listener.ExceptionListener;

public class ConnectionExceptionListener implements ExceptionListener {

	private AbstractClientConnectionControl connectionControl;
	
	private String brokerUrl;
	
	public ConnectionExceptionListener(AbstractClientConnectionControl connectionControl, String brokerUrl) {
		this.connectionControl = connectionControl;
		this.brokerUrl = brokerUrl;
	}
	
	@Override
	public void onException(HippoException exception) {
		connectionControl.exceptionDispose(brokerUrl);
	}

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

}
