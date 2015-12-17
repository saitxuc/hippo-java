package com.hippoconsoleweb.model;

import java.io.Serializable;
import java.util.Date;

public class ServerInfoDo implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public String server_id; // ip + port
    public int last_time;
    public int status;
    public String store;
    public String memory;
    public String brokerName;
    public String brokerVersion;
    public String clusterName;
    public long clusterId;
    public String port;
    public String bucketCount;
    public Date createDate;
    public Date modifyDate;
    public int rows;
    public int offset;
    public int df;
    public String ip;
    public long id;
    public String jmxPort;
    
    
    public String getJmxPort() {
		return jmxPort;
	}
	public void setJmxPort(String jmxPort) {
		this.jmxPort = jmxPort;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getDf() {
		return df;
	}
	public void setDf(int df) {
		this.df = df;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public long getClusterId() {
		return clusterId;
	}
	public void setClusterId(long clusterId) {
		this.clusterId = clusterId;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getBucketCount() {
		return bucketCount;
	}
	public void setBucketCount(String bucketCount) {
		this.bucketCount = bucketCount;
	}
	public Date getCreateDate() {
		return createDate;
	}
	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}
	public Date getModifyDate() {
		return modifyDate;
	}
	public void setModifyDate(Date modifyDate) {
		this.modifyDate = modifyDate;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getStore() {
		return store;
	}
	public void setStore(String store) {
		this.store = store;
	}
	public String getMemory() {
		return memory;
	}
	public void setMemory(String memory) {
		this.memory = memory;
	}
	public String getBrokerName() {
		return brokerName;
	}
	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}
	public String getBrokerVersion() {
		return brokerVersion;
	}
	public void setBrokerVersion(String brokerVersion) {
		this.brokerVersion = brokerVersion;
	}
	public String getServer_id() {
		return server_id;
	}
	public void setServer_id(String server_id) {
		this.server_id = server_id;
	}
	public int getLast_time() {
		return last_time;
	}
	public void setLast_time(int last_time) {
		this.last_time = last_time;
	}
	public void setStatus(short status) {
		this.status = status;
	}
	public int getRows() {
		return rows;
	}
	public void setRows(int rows) {
		this.rows = rows;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
    
    
}
