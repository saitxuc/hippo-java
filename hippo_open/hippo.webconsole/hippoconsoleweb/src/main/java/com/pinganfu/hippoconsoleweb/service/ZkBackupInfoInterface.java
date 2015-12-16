package com.pinganfu.hippoconsoleweb.service;

import java.util.List;

import com.pinganfu.hippoconsoleweb.model.ZkClusterBackUpInfoBean;
import com.pinganfu.hippoconsoleweb.model.ZkDataServersInfoBean;
import com.pinganfu.hippoconsoleweb.model.ZkTablesInfoBean;

public interface ZkBackupInfoInterface {

	public List<ZkClusterBackUpInfoBean> loadBackUpList(int offset,int rows,ZkClusterBackUpInfoBean info)throws Exception;
	
	public int loadBackUpCount(ZkClusterBackUpInfoBean info)throws Exception;
	
	public List<ZkTablesInfoBean> loadTablesInfoByClusterId(long clusterId)throws Exception;
	
	public List<ZkDataServersInfoBean> loadDataServersInfoByClusterId(long clusterId)throws Exception;
}
