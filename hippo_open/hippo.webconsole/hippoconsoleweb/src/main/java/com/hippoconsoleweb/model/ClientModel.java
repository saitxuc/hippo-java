package com.hippoconsoleweb.model;

import java.io.Serializable;

public class ClientModel implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String clientId;
	private String remoteAddress;
	private String active;
	private String blocked;
	private String connected;
	private String slow;
	private String objectName;
	
	
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public String getActive() {
		return active;
	}
	public void setActive(String active) {
		this.active = active;
	}
	public String getBlocked() {
		return blocked;
	}
	public void setBlocked(String blocked) {
		this.blocked = blocked;
	}
	public String getConnected() {
		return connected;
	}
	public void setConnected(String connected) {
		this.connected = connected;
	}
	public String getSlow() {
		return slow;
	}
	public void setSlow(String slow) {
		this.slow = slow;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public String getRemoteAddress() {
		return remoteAddress;
	}
	public void setRemoteAddress(String remoteAddress) {
		this.remoteAddress = remoteAddress;
	}
	
	
}
