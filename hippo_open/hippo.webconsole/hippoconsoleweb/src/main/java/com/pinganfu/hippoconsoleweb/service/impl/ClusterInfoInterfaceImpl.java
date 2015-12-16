package com.pinganfu.hippoconsoleweb.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.BeanUtils;

import com.pinganfu.hippoconsoleweb.dal.ClusterInfoDao;
import com.pinganfu.hippoconsoleweb.model.ClusterInfoBean;
import com.pinganfu.hippoconsoleweb.model.ClusterInfoDo;
import com.pinganfu.hippoconsoleweb.service.ClusterInfoInterface;
import com.pinganfu.hippoconsoleweb.service.ConsoleManagerService;

public class ClusterInfoInterfaceImpl implements ClusterInfoInterface  {

	@Resource
	private ClusterInfoDao ClusterInfoDao;
	@Resource
	private ConsoleManagerService consoleManagerService;
	
	@Override
	public List<ClusterInfoBean> loadClusterInfoList(ClusterInfoBean model)throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		List<ClusterInfoDo> list = ClusterInfoDao.loadClusterInfoList(info);
		List<ClusterInfoBean> listF = new ArrayList<ClusterInfoBean>();
		if(list !=null && list.size()>0){
			for(ClusterInfoDo infoDo : list){
				ClusterInfoBean bean = new ClusterInfoBean();
				BeanUtils.copyProperties(infoDo, bean);
				listF.add(bean);
			}
		}
		return listF;
	}
	
	
	@Override
	public int insertClusterInfo(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		return ClusterInfoDao.insertClusterInfo(info);
	}
	
	@Override
	public ClusterInfoBean findClusterInfo(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		ClusterInfoDo infoDo = ClusterInfoDao.findClusterInfo(info);
		ClusterInfoBean target = new ClusterInfoBean();
		BeanUtils.copyProperties(infoDo, target);
		return target;
	}
	
	@Override
	public int findClusterInfoCount(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		return ClusterInfoDao.findClusterInfoCount(info);
	}
	
	@Override
	public int delClusterInfo(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		return ClusterInfoDao.delClusterInfo(info);
	}
	
	@Override
	public int editClusterInfo(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		return ClusterInfoDao.editClusterInfo(info);
	}
	
	@Override
	public int sendClusterInfo(ClusterInfoBean model) throws Exception {
		ClusterInfoDo info = new ClusterInfoDo();
		BeanUtils.copyProperties(model, info);
		return ClusterInfoDao.editClusterInfo(info);
	}
	
	@Override
	public void addSubscribeClidChangs(String clusterName, String path,String zkAddress)throws Exception {
		consoleManagerService.subscribeClusterListener(zkAddress, clusterName, new Object());
	}
	
}
