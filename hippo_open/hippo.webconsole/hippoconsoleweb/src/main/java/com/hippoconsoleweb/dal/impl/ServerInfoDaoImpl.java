package com.hippoconsoleweb.dal.impl;

import java.util.List;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.hippoconsoleweb.dal.ServerInfoDao;
import com.hippoconsoleweb.model.ServerInfoDo;

public class ServerInfoDaoImpl extends SqlSessionDaoSupport  implements ServerInfoDao  {

	
	@Override
	public int insertServerInfo(ServerInfoDo info) throws Exception {
		int rows = this.getSqlSession().insert("insertServerInfo", info);
		return rows;
	}
	
	@Override
	public List<ServerInfoDo> loadServerInfoList(ServerInfoDo info)throws Exception {
		List<ServerInfoDo> list = this.getSqlSession().selectList("loadServerInfoList", info);
		return list;
	}
	
	@Override
	public ServerInfoDo findServerInfo(ServerInfoDo info) throws Exception {
		ServerInfoDo infoDo = (ServerInfoDo)this.getSqlSession().selectOne("findServerInfo", info);
		return infoDo;
	}
	
	@Override
	public int findServerInfoCount(ServerInfoDo info) throws Exception {
		return (Integer)this.getSqlSession().selectOne("findServerInfoCount", info);
	}
	
	@Override
	public int delServerInfo(ServerInfoDo info) throws Exception {
		return this.getSqlSession().update("delServerInfo", info);
	}
	
	@Override
	public int editServerInfo(ServerInfoDo info) throws Exception {
		return this.getSqlSession().update("editServerInfo", info);
	}
}
