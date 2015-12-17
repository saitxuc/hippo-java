package com.hippoconsoleweb.model;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class ZkClusterBackUpInfoBean implements Serializable{

	private static final long serialVersionUID = 1L;
	private long id;
	private String clusterName;
	private Date createdate;
	private int version;
	private int df;
	private List<ZkDataServersInfoBean> dataservers;
	private List<ZkTablesInfoBean> tables;
	private String migration;
	private String config;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public Date getCreatedate() {
		return createdate;
	}
	public void setCreatedate(Date createdate) {
		this.createdate = createdate;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public int getDf() {
		return df;
	}
	public void setDf(int df) {
		this.df = df;
	}
	public List<ZkDataServersInfoBean> getDataservers() {
		return dataservers;
	}
	public void setDataservers(List<ZkDataServersInfoBean> dataservers) {
		this.dataservers = dataservers;
	}
	public List<ZkTablesInfoBean> getTables() {
		return tables;
	}
	public void setTables(List<ZkTablesInfoBean> tables) {
		this.tables = tables;
	}
	public String getMigration() {
		return migration;
	}
	public void setMigration(String migration) {
		this.migration = migration;
	}
	public String getConfig() {
		return config;
	}
	public void setConfig(String config) {
		this.config = config;
	}
	
	
}
