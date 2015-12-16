package com.pinganfu.hippo.common;

import java.io.Serializable;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class Result implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -144482153569544226L;
	private boolean isSuccess;
	private Serializable key;
	private Serializable value;
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}

	public Serializable getKey() {
		return key;
	}
	public void setKey(Serializable key) {
		this.key = key;
	}
	public Serializable getValue() {
		return value;
	}
	public void setValue(Serializable value) {
		this.value = value;
	}
	public Result(boolean isSuccess, Serializable key, Serializable value) {
		this.isSuccess = isSuccess;
		this.key = key;
		this.value = value;
	}
	public Result(boolean isSuccess, Serializable key) {
		this(isSuccess,key,null);
	}
	
}
