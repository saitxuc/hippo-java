package com.hippo.client.listener;

import com.hippo.client.transport.AbstractClientConnectionControl;
import com.hippo.common.exception.HippoException;
import com.hippo.common.listener.ExceptionListener;

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
