package com.pinganfu.hippoconsoleweb.service;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;

public interface ServerInfoInterface {

	public List<ServerInfoBean> loadServerInfoList(ServerInfoBean info)throws Exception;
	
	public int insertServerInfo(ServerInfoBean info)throws Exception;
	
	public ServerInfoBean findServerInfo(ServerInfoBean info)throws Exception;
	
	public int findServerInfoCount(ServerInfoBean info)throws Exception;
	
	public int delServerInfo(ServerInfoBean info)throws Exception;
	
	public int editServerInfo(ServerInfoBean info)throws Exception;
	
	public Map<Integer, Vector<String>> loadServerTable(String address,String clusterName)throws Exception;
	
	public List<ServerInfoBean> loadServerInfoByZk(String clusterName, String address)throws Exception;
	
	public ServerInfoBean loadBucketCount(String clusterName, ServerInfoBean info, String address)throws Exception;
	
	public ServerInfoBean getJmxValue(ServerInfoBean info)throws Exception;
}
