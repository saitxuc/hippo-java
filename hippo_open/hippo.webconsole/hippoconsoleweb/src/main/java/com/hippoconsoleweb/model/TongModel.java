package com.hippoconsoleweb.model;

import java.io.Serializable;

public class TongModel implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String tongMark;
	private String tongPerformance;
	private int isMaster;
	private int level;
	public String getTongMark() {
		return tongMark;
	}
	public void setTongMark(String tongMark) {
		this.tongMark = tongMark;
	}
	public String getTongPerformance() {
		return tongPerformance;
	}
	public void setTongPerformance(String tongPerformance) {
		this.tongPerformance = tongPerformance;
	}
	public int getIsMaster() {
		return isMaster;
	}
	public void setIsMaster(int isMaster) {
		this.isMaster = isMaster;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	
	

	
}
