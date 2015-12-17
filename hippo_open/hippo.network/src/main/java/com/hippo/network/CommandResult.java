package com.hippo.network;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author saitxuc
 * 2015-4-1
 */
public class CommandResult implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8468762432143162550L;
	protected boolean isSuccess;
	protected String errorCode;
	protected Serializable message;
	protected byte[] data;
	
	
	private Map<String, String> attrMap = new HashMap<String, String>();
	
	public CommandResult() {
		
	}
	
	public CommandResult(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	
	public CommandResult(boolean isSuccess, Serializable message, int version) {
		this.isSuccess = isSuccess;
		this.message = message;
		this.attrMap.put("version", String.valueOf(version));
	}
	
	public CommandResult(boolean isSuccess, byte[] data, int version, long expireTime) {
        this.isSuccess = isSuccess;
        this.data = data;
        this.attrMap.put("version", String.valueOf(version));
        this.attrMap.put("expireTime", String.valueOf(expireTime));
    }
	
	public CommandResult(boolean isSuccess, String errorCode, 
			Serializable message) {
		this.isSuccess = isSuccess;
		this.errorCode = errorCode;
		this.message = message;
	}
	
	public CommandResult(boolean isSuccess, byte[] data, int version) {
		this.isSuccess = isSuccess;
		this.data = data;
		this.attrMap.put("version", String.valueOf(version));
	}
	
	public CommandResult(boolean isSuccess, Serializable message) {
		this.isSuccess = isSuccess;
		this.message = message;
	}
	
	public CommandResult(boolean isSuccess, String errorCode, 
			Serializable message, byte[] data, int version) {
		this.isSuccess = isSuccess;
		this.errorCode = errorCode;
		this.message = message;
		this.data = data;
		this.attrMap.put("version", String.valueOf(version));
	}
	
	public boolean isSuccess() {
		return isSuccess;
	}

	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public Serializable getMessage() {
		return message;
	}

	public void setMessage(Serializable message) {
		this.message = message;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public Map<String, String> getAttrMap() {
		return attrMap;
	}

	public void setAttrMap(Map<String, String> attrMap) {
		this.attrMap = attrMap;
	}
	
	public void putAttribute(String name, String value) {
		this.attrMap.put(name, value);
	}
	
	public String getSttribute(String name) {
		return this.attrMap.get(name);
	}
	
}
