package com.hippoconsoleweb.service.impl;

import java.util.ArrayList;
import java.util.List;

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
import com.hippoconsoleweb.service.ZkBackupInfoInterface;

public class ZkBackupInfoInterfaceImpl implements ZkBackupInfoInterface  {
	
	@Resource
	private ZkClusterInfoDao zkClusterInfoDao;
	@Resource 
	private ZkTablesInfoDao zkTablesInfoDao;
	@Resource 
	private ZkDataServersDao zkDataServersDao;

	@Override
	public int loadBackUpCount(ZkClusterBackUpInfoBean info) throws Exception {
		ZkClusterBackUpInfoDo infoDo = new ZkClusterBackUpInfoDo();
		infoDo.setClusterName(info.getClusterName());
		infoDo.setDf(info.getDf());
		infoDo.setVersion(info.getVersion());
		int count = zkClusterInfoDao.selectBackUpCount(infoDo);
		return count;
	}
	
	@Override
	public List<ZkClusterBackUpInfoBean> loadBackUpList(int offset, int rows,ZkClusterBackUpInfoBean info) throws Exception {
		List<ZkClusterBackUpInfoBean> returnList = new ArrayList<ZkClusterBackUpInfoBean>();
		ZkClusterBackUpInfoDo infoDo = new ZkClusterBackUpInfoDo();
		infoDo.setClusterName(info.getClusterName());
		infoDo.setDf(info.getDf());
		infoDo.setVersion(info.getVersion());
		List<ZkClusterBackUpInfoDo> list = zkClusterInfoDao.selectBackUpList(infoDo, rows, offset);
		if(list !=null && list.size()>0){
			for(ZkClusterBackUpInfoDo backUp : list){
				ZkClusterBackUpInfoBean returnBean = new ZkClusterBackUpInfoBean();
				returnBean.setClusterName(backUp.getClusterName());
				returnBean.setConfig(backUp.getConfig());
				returnBean.setCreatedate(backUp.getCreatedate());
				returnBean.setDf(backUp.getDf());
				returnBean.setMigration(backUp.getMigration());
				returnBean.setVersion(backUp.getVersion());
				returnBean.setId(backUp.getId());
				returnList.add(returnBean);
			}
		}else{
			return returnList;
		}
		
		return returnList;
	}
	
	@Override
	public List<ZkTablesInfoBean> loadTablesInfoByClusterId(long clusterId)throws Exception {
		List<ZkTablesInfoBean> tableList = new ArrayList<ZkTablesInfoBean>();
		ZkTablesInfoDo tablesDo = new ZkTablesInfoDo();
		tablesDo.setZkClusterId(clusterId);
		tablesDo.setDf(0);
		List<ZkTablesInfoDo> list = zkTablesInfoDao.selectTablesList(tablesDo);
		if(list !=null && list.size()>0){
			for(ZkTablesInfoDo infoDo:list){
				ZkTablesInfoBean bean = new ZkTablesInfoBean();
				bean.setId(infoDo.getId());
				bean.setContent(infoDo.getContent());
				bean.setCreatedate(infoDo.getCreatedate());
				bean.setDf(infoDo.getDf());
				bean.setType(infoDo.getType());
				bean.setZkClusterId(infoDo.getZkClusterId());
				tableList.add(bean);
			}
		}
		return tableList;
	}
	
	@Override
	public List<ZkDataServersInfoBean> loadDataServersInfoByClusterId(long clusterId) throws Exception {
		List<ZkDataServersInfoBean> dataList = new ArrayList<ZkDataServersInfoBean>();
		ZkDataServersInfoDo dataInfo = new ZkDataServersInfoDo();
		dataInfo.setZkClusterId(clusterId);
		dataInfo.setDf(0);
		List<ZkDataServersInfoDo> list = zkDataServersDao.selectDataServersList(dataInfo);
		if(list !=null && list.size()>0){
			for(ZkDataServersInfoDo infoDo:list){
				ZkDataServersInfoBean bean = new ZkDataServersInfoBean();
				bean.setContent(infoDo.getContent());
				bean.setCreatedate(infoDo.getCreatedate());
				bean.setNetworkPort(infoDo.getNetworkPort());
				bean.setZkClusterId(infoDo.getZkClusterId());
				dataList.add(bean);
			}
		}
		return dataList;
	}
	
}
