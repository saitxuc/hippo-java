package com.pinganfu.hippoconsoleweb.dal.impl;

import java.util.List;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.pinganfu.hippoconsoleweb.dal.ZkTablesInfoDao;
import com.pinganfu.hippoconsoleweb.model.ZkTablesInfoDo;

public class ZkTablesInfoDaoImpl extends SqlSessionDaoSupport implements ZkTablesInfoDao{
	
	
	@Override
	public List<ZkTablesInfoDo> selectTablesList(ZkTablesInfoDo info) throws Exception {
		return this.getSqlSession().selectList("selectTablesList", info);
	}
	
	@Override
	public ZkTablesInfoDo selectOneTables(ZkTablesInfoDo info) throws Exception {
		
		return this.getSqlSession().selectOne("selectOneTables", info);
	}
	
	@Override
	public int insertTablesList(ZkTablesInfoDo info) throws Exception {
		return this.getSqlSession().insert("insertTablesList", info);
	}
	
	@Override
	public int updateTablesList(ZkTablesInfoDo info) throws Exception {
		return this.getSqlSession().update("updateTablesList", info);
	}
	
	@Override
	public int deleteTables(ZkTablesInfoDo info) throws Exception {
		
		return this.getSqlSession().update("deleteTables", info);
	}

}
