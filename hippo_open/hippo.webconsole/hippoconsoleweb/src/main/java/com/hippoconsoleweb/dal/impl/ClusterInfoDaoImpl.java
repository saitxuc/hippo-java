package com.hippoconsoleweb.dal.impl;

import java.util.List;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.hippoconsoleweb.dal.ClusterInfoDao;
import com.hippoconsoleweb.model.ClusterInfoDo;

public class ClusterInfoDaoImpl extends SqlSessionDaoSupport implements ClusterInfoDao  {
	
	@Override
	public int insertClusterInfo(ClusterInfoDo info) throws Exception {
		int rows = this.getSqlSession().insert("ClusterInfoDo.insertClusterInfo", info);
		return rows;
	}
	
	@Override
	public List<ClusterInfoDo> loadClusterInfoList(ClusterInfoDo info)throws Exception {
		List<ClusterInfoDo> list = this.getSqlSession().selectList("ClusterInfoDo.loadClusterInfoList", info);
		return list;
	}
	
	@Override
	public ClusterInfoDo findClusterInfo(ClusterInfoDo info) throws Exception {
		ClusterInfoDo infoDo = (ClusterInfoDo)this.getSqlSession().selectOne("ClusterInfoDo.findClusterInfo", info);
		return infoDo;
	}
	
	@Override
	public int findClusterInfoCount(ClusterInfoDo info) throws Exception {
		return (Integer)this.getSqlSession().selectOne("ClusterInfoDo.findClusterInfoCount", info);
	}
	
	@Override
	public int delClusterInfo(ClusterInfoDo info) throws Exception {
		return this.getSqlSession().update("ClusterInfoDo.delClusterInfo", info);
	}
	
	@Override
	public int editClusterInfo(ClusterInfoDo info) throws Exception {
		return this.getSqlSession().update("ClusterInfoDo.editClusterInfo", info);
	}
}
