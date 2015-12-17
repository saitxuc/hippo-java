package com.hippoconsoleweb.dal;

import java.util.List;

import com.hippoconsoleweb.model.ZkTablesInfoDo;

public interface ZkTablesInfoDao {

	public List<ZkTablesInfoDo> selectTablesList(ZkTablesInfoDo info)throws Exception;
	
	public int insertTablesList(ZkTablesInfoDo info)throws Exception;
	
	public int updateTablesList(ZkTablesInfoDo info)throws Exception;
	
	public ZkTablesInfoDo selectOneTables(ZkTablesInfoDo info)throws Exception;
	
	public int deleteTables(ZkTablesInfoDo info)throws Exception;
}
