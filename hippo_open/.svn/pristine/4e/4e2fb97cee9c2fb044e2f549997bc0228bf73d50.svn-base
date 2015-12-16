package com.pinganfu.hippo.network.command;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Optional;

/**
 * 
 * @author saitxuc
 * write 2014-6-30
 */
public class Command implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final String NO_DATA = "";

	private String action;
	protected Map<String, String> headers = new HashMap<String, String>();
	private Serializable content = NO_DATA;
	
	@Optional(value = "")
	private byte[] data = null;
	
	protected void copy(Command copy) {
		copy.action = this.action;
		
		Map<String, String> copyHeaders = new HashMap<String, String>();
		copyHeaders.putAll(headers);
		copy.headers = copyHeaders;
		
		copy.content = this.content;
		copy.data = this.data;
	}
	
	public String getAction() {
		return action;
	}
	
	public void setAction(String action) {
		this.action = action;
	}
	
	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	public Serializable getContent() {
		return this.content;
	}

	public void setContent(Serializable content) {
		this.content = content;
	}

	public String getHeadValue(String key){
		return this.headers.get(key);
	}
	public String putHeadValue(String key,String value){
		return this.headers.put(key, value);
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
}
