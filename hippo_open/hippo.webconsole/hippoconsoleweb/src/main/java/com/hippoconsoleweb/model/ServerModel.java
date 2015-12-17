package com.hippoconsoleweb.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public class ServerModel implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String serverName; //--
	private String serverRack; //--
	private BigDecimal serverPerformance; //--
	private String tongType;   //--
	private List<TongModel> tongList;
	
	public String getServerName() {
		return serverName;
	}
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	public String getServerRack() {
		return serverRack;
	}
	public void setServerRack(String serverRack) {
		this.serverRack = serverRack;
	}
	public String getTongType() {
		return tongType;
	}
	public void setTongType(String tongType) {
		this.tongType = tongType;
	}
	public List<TongModel> getTongList() {
		return tongList;
	}
	public void setTongList(List<TongModel> tongList) {
		this.tongList = tongList;
	}
	public BigDecimal getServerPerformance() {
		return serverPerformance;
	}
	public void setServerPerformance(BigDecimal serverPerformance) {
		this.serverPerformance = serverPerformance;
	}
	
	
	
}
