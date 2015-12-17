package com.hippoconsoleweb.model;

import java.io.Serializable;

public class ZkManageTree implements Serializable{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String text;
	private String state;
	private boolean checked;
	private String attributes;
	private ZkManageTree children;
	private String memo;
	
	
	public String getMemo() {
		return memo;
	}
	public void setMemo(String memo) {
		this.memo = memo;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public boolean isChecked() {
		return checked;
	}
	public void setChecked(boolean checked) {
		this.checked = checked;
	}
	public String getAttributes() {
		return attributes;
	}
	public void setAttributes(String attributes) {
		this.attributes = attributes;
	}
	public ZkManageTree getChildren() {
		return children;
	}
	public void setChildren(ZkManageTree children) {
		this.children = children;
	}
	
	
}
