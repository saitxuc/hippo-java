package com.hippoconsoleweb.service;

import java.util.List;

import com.hippoconsoleweb.model.ZkClusterBackUpInfoBean;
import com.hippoconsoleweb.model.ZkDataServersInfoBean;
import com.hippoconsoleweb.model.ZkTablesInfoBean;

public interface ZkBackupInfoInterface {

	public List<ZkClusterBackUpInfoBean> loadBackUpList(int offset,int rows,ZkClusterBackUpInfoBean info)throws Exception;
	
	public int loadBackUpCount(ZkClusterBackUpInfoBean info)throws Exception;
	
	public List<ZkTablesInfoBean> loadTablesInfoByClusterId(long clusterId)throws Exception;
	
	public List<ZkDataServersInfoBean> loadDataServersInfoByClusterId(long clusterId)throws Exception;
}
