package com.hippoconsoleweb.dal.impl;

import java.util.List;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.hippoconsoleweb.dal.ZkDataServersDao;
import com.hippoconsoleweb.model.ZkDataServersInfoDo;

public class ZkDataServersDaoImpl extends SqlSessionDaoSupport implements ZkDataServersDao {

	
	@Override
	public int insertDataServersList(ZkDataServersInfoDo info) throws Exception {
		int rows = this.getSqlSession().insert("insertDataServersList", info);
		return rows;
	}
	
	@Override
	public List<ZkDataServersInfoDo> selectDataServersList(ZkDataServersInfoDo info) throws Exception {
		List<ZkDataServersInfoDo> list = this.getSqlSession().selectList("selectDataServersList", info);
		return list;
	}
	
	@Override
	public ZkDataServersInfoDo selectOneDataServers(ZkDataServersInfoDo info)throws Exception {
		return this.getSqlSession().selectOne("selectOneDataServers", info);
	}
	
	@Override
	public int updateDataServersList(ZkDataServersInfoDo info) throws Exception {
		return this.getSqlSession().update("updateDataServersList", info);
	}

	@Override
	public int deleteDataServers(ZkDataServersInfoDo info) throws Exception {
		return this.getSqlSession().update("deleteDataServers", info);
	}
}
