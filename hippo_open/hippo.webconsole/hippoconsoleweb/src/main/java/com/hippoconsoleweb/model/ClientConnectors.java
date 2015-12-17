package com.hippoconsoleweb.model;

import java.io.Serializable;

public class ClientConnectors implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String connStarted;
	private String connEnabled;
	private String objectName;
	private String startException;
	
	
	public String getStartException() {
		return startException;
	}
	public void setStartException(String startException) {
		this.startException = startException;
	}
	public String getConnStarted() {
		return connStarted;
	}
	public void setConnStarted(String connStarted) {
		this.connStarted = connStarted;
	}
	public String getConnEnabled() {
		return connEnabled;
	}
	public void setConnEnabled(String connEnabled) {
		this.connEnabled = connEnabled;
	}
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	
	
}
