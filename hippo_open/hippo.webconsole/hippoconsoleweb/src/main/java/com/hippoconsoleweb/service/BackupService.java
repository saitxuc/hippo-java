package com.hippoconsoleweb.service;

import javax.annotation.Resource;

import com.hippoconsoleweb.dal.ZkClusterInfoDao;
import com.hippoconsoleweb.dal.ZkDataServersDao;
import com.hippoconsoleweb.dal.ZkTablesInfoDao;
import com.hippoconsoleweb.model.ZkClusterBackUpInfoBean;
import com.hippoconsoleweb.model.ZkClusterBackUpInfoDo;
import com.hippoconsoleweb.model.ZkDataServersInfoBean;
import com.hippoconsoleweb.model.ZkDataServersInfoDo;
import com.hippoconsoleweb.model.ZkTablesInfoBean;
import com.hippoconsoleweb.model.ZkTablesInfoDo;

public class BackupService {

	@Resource
	private ZkClusterInfoDao zkClusterInfoDao;
	@Resource
	private ZkDataServersDao zkDataServersDao;
	@Resource
	private ZkTablesInfoDao zkTablesInfoDao;
	
	public void save(ZkClusterBackUpInfoBean bean)throws Exception{
		ZkClusterBackUpInfoDo info = copyBean(bean);
		long clusterId = zkClusterInfoDao.insertBackUpList(info);
		info.setId(clusterId);
		if(bean.getDataservers() !=null && bean.getDataservers().size()>0){
			for(ZkDataServersInfoBean infoBean : bean.getDataservers()){
				ZkDataServersInfoDo infoDo = copyBean(infoBean,info);
				zkDataServersDao.insertDataServersList(infoDo);
			}
		}
		
		if(bean.getTables() !=null && bean.getTables().size()>0){
			for(ZkTablesInfoBean infoBean : bean.getTables()){
				ZkTablesInfoDo infoDo = copyBean(infoBean,info);
				zkTablesInfoDao.insertTablesList(infoDo);
			}
		}
	}
	
	
	/**
	 * return zk backup version
	 * @param cluster
	 * @return
	 * @throws Exception
	 */
	public int getVersion(String cluster)throws Exception{
		int version = 0;
		ZkClusterBackUpInfoDo info = new ZkClusterBackUpInfoDo();
		info.setClusterName(cluster);
		info.setDf(0);
		version = zkClusterInfoDao.loadBackupVersion(info);
		version++;
		return version;
	}
	
	private ZkClusterBackUpInfoDo copyBean(ZkClusterBackUpInfoBean bean)throws Exception{
		ZkClusterBackUpInfoDo info = new ZkClusterBackUpInfoDo();
		info.setClusterName(bean.getClusterName());
		info.setConfig(bean.getConfig());
		info.setCreatedate(bean.getCreatedate());
		info.setDf(bean.getDf());
		info.setVersion(bean.getVersion());
		info.setMigration(bean.getMigration());
		return info;
	}
	
	private ZkDataServersInfoDo copyBean(ZkDataServersInfoBean bean,ZkClusterBackUpInfoDo backUpInfo)throws Exception{
		ZkDataServersInfoDo info = new ZkDataServersInfoDo();
		info.setZkClusterId(backUpInfo.getId());
		info.setCreatedate(bean.getCreatedate());
		info.setDf(bean.getDf());
		info.setNetworkPort(bean.getNetworkPort());
		info.setContent(bean.getContent());
		return info;
	}
	
	private  ZkTablesInfoDo copyBean(ZkTablesInfoBean bean,ZkClusterBackUpInfoDo backUpInfo)throws Exception{
		ZkTablesInfoDo info = new ZkTablesInfoDo();
		info.setContent(bean.getContent());
		info.setCreatedate(bean.getCreatedate());
		info.setDf(bean.getDf());
		info.setType(bean.getType());
		info.setZkClusterId(backUpInfo.getId());
		return info;
	}
}
