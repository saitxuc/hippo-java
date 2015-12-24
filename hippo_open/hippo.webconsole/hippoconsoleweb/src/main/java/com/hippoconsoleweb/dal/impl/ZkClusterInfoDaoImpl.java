package com.hippoconsoleweb.dal.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.hippoconsoleweb.dal.ZkClusterInfoDao;
import com.hippoconsoleweb.model.ZkClusterBackUpInfoDo;

public class ZkClusterInfoDaoImpl extends SqlSessionDaoSupport  implements ZkClusterInfoDao {


	@Override
	public List<ZkClusterBackUpInfoDo> selectBackUpList(ZkClusterBackUpInfoDo info, int rows, int offset) throws Exception {
		Map<String,Object> map = new HashMap<String, Object>();
		map.put("rows", rows);
		map.put("offset", offset);
		map.put("df", 0);
		map.put("clusterName", info.getClusterName());
		map.put("version", info.getVersion());
		List<ZkClusterBackUpInfoDo> list = this.getSqlSession().selectList("selectBackUpList", map);
		return list;
	}
	
	@Override
	public ZkClusterBackUpInfoDo selectOneBackUp(ZkClusterBackUpInfoDo info)throws Exception {
		ZkClusterBackUpInfoDo infoDo = this.getSqlSession().selectOne("selectOneBackUp", info);
		return infoDo;
	}
	
	@Override
	public Long insertBackUpList(ZkClusterBackUpInfoDo info) throws Exception {
		int rows = this.getSqlSession().insert("insertBackUpList", info);
		return info.getId();
	}
	
	@Override
	public int updateBackUpList(ZkClusterBackUpInfoDo info) throws Exception {
		int rows = this.getSqlSession().update("updateBackUpList", info);
		return rows;
	}
	
	@Override
	public int deleteBackUp(ZkClusterBackUpInfoDo info) throws Exception {
		int rows  = this.getSqlSession().update("deleteBackUp", info);
		return rows;
	}
	
	@Override
	public int selectBackUpCount(ZkClusterBackUpInfoDo info) throws Exception {
		int count = (Integer)this.getSqlSession().selectOne("selectBackUpCount", info);
		return count;
	}
	
	@Override
	public int loadBackupVersion(ZkClusterBackUpInfoDo info) throws Exception {
		int version = (Integer)this.getSqlSession().selectOne("loadBackupVersion", info);
		return version;
	}

}
