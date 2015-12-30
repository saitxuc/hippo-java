package com.hippo.broker.transport;

/**
 * 
 * @author saitxuc
 * write 2014-8-8
 */
public class TransportConnectorConfig {
	
	private String uri;
	
	public TransportConnectorConfig(String uri) {
		this.uri = uri;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	
}
