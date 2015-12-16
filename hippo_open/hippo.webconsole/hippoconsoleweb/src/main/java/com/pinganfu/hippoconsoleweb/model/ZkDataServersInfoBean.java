package com.pinganfu.hippoconsoleweb.model;

import java.io.Serializable;
import java.util.Date;

public class ZkDataServersInfoBean implements Serializable {

	private static final long serialVersionUID = 1L;
	private long id;
	private long zkClusterId;
	private String networkPort;
	private String content;
	private int df;
	private Date createdate;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getZkClusterId() {
		return zkClusterId;
	}
	public void setZkClusterId(long zkClusterId) {
		this.zkClusterId = zkClusterId;
	}
	public String getNetworkPort() {
		return networkPort;
	}
	public void setNetworkPort(String networkPort) {
		this.networkPort = networkPort;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public int getDf() {
		return df;
	}
	public void setDf(int df) {
		this.df = df;
	}
	public Date getCreatedate() {
		return createdate;
	}
	public void setCreatedate(Date createdate) {
		this.createdate = createdate;
	}
	
	
	
}
