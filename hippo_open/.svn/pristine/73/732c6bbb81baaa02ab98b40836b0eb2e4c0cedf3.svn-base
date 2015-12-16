package com.pinganfu.hippoconsoleweb.service;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.pinganfu.hippoconsoleweb.model.ClusterInfoBean;
import com.pinganfu.hippoconsoleweb.model.ServerInfo;
import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.model.ZkManageTree;

public interface ZkManageInterface {

	public List<Map<String ,Object>> loadZkList(ZkManageTree zkDo)throws Exception;
	
	public String loadZkRead(ZkManageTree zkDo)throws Exception;
	
	public boolean ZkWrite(ZkManageTree zkDo)throws Exception;
	
	public boolean ZkCreate(ZkManageTree zkDo)throws Exception;
	
	public boolean ZkDelete(ZkManageTree zkDo)throws Exception;
	
	public Map<Integer,Vector<String>> loadTable(String clusterName)throws Exception;
	
	public List<ServerInfo> loadDsList(int start,int number)throws Exception;
	
	public int loadDsListSize()throws Exception;
	
	public List<Map<String, String>> loadCluseterMenuZk()throws Exception;
	
	public List<Map<String, String>> loadCluseterMenuBase()throws Exception;
	
	public List<Map<String, String>> loadCluseterMenuBaseSelected()throws Exception;
	
	public List<Map<String, String>> loadServerMenuZk(String clusterName)throws Exception;
	
	public ServerInfoBean loadServerBucketInfo(String clusterName,String server_id,String jmxPort,String brokerName)throws Exception;
	
	public boolean pathExists(String clusterName,String server_id)throws Exception;
	
	public List<Map<String ,Object>> loadZkRootList(ZkManageTree zkDo)throws Exception;
	
	public String loadDsChangeType()throws Exception;
	
	public boolean sendClusterToZk(ClusterInfoBean info)throws Exception;
	
	public boolean zkDataRest(String clusterName)throws Exception;
}
