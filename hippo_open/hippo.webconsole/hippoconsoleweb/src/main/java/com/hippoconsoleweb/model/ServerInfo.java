package com.hippoconsoleweb.model;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class ServerInfo implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public String server_id; // ip + port
    public int last_time;
    public int status;
    public String store;
    public String memory;
    public String brokerName;
    public String brokerVersion;
    public String clusterName;
    public String clusterId;
    public String port;
    public String bucketCount;
    public String masterBucket;
    public String slaveBucket;
    public Date createDate;
    public Date modifyDate;
    public int rows;
    public int offset;
    public String dataDirectory;
    public String started;
    public String memoryLimit;
    public String memoryPercentUsage;
    public String storeLimit;
    public String storePercentUsage;
    public String currentUsedCapacity;
    public String engineData;
    public String engineName;
    public String engineSize;
    public List<ClientConnectors> conn;
    public List<ClientModel> client;
    
    public String getDataDirectory() {
		return dataDirectory;
	}
	public void setDataDirectory(String dataDirectory) {
		this.dataDirectory = dataDirectory;
	}
	public String getStarted() {
		return started;
	}
	public void setStarted(String started) {
		this.started = started;
	}
	public String getMemoryLimit() {
		return memoryLimit;
	}
	public void setMemoryLimit(String memoryLimit) {
		this.memoryLimit = memoryLimit;
	}
	public String getMemoryPercentUsage() {
		return memoryPercentUsage;
	}
	public void setMemoryPercentUsage(String memoryPercentUsage) {
		this.memoryPercentUsage = memoryPercentUsage;
	}
	public String getStoreLimit() {
		return storeLimit;
	}
	public void setStoreLimit(String storeLimit) {
		this.storeLimit = storeLimit;
	}
	public String getStorePercentUsage() {
		return storePercentUsage;
	}
	public void setStorePercentUsage(String storePercentUsage) {
		this.storePercentUsage = storePercentUsage;
	}
	public String getCurrentUsedCapacity() {
		return currentUsedCapacity;
	}
	public void setCurrentUsedCapacity(String currentUsedCapacity) {
		this.currentUsedCapacity = currentUsedCapacity;
	}
	public String getEngineData() {
		return engineData;
	}
	public void setEngineData(String engineData) {
		this.engineData = engineData;
	}
	public String getEngineName() {
		return engineName;
	}
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}
	public String getEngineSize() {
		return engineSize;
	}
	public void setEngineSize(String engineSize) {
		this.engineSize = engineSize;
	}
	public List<ClientConnectors> getConn() {
		return conn;
	}
	public void setConn(List<ClientConnectors> conn) {
		this.conn = conn;
	}
	public List<ClientModel> getClient() {
		return client;
	}
	public void setClient(List<ClientModel> client) {
		this.client = client;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getClusterId() {
		return clusterId;
	}
	public void setClusterId(String clusterId) {
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
	public String getMasterBucket() {
		return masterBucket;
	}
	public void setMasterBucket(String masterBucket) {
		this.masterBucket = masterBucket;
	}
	public String getSlaveBucket() {
		return slaveBucket;
	}
	public void setSlaveBucket(String slaveBucket) {
		this.slaveBucket = slaveBucket;
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
