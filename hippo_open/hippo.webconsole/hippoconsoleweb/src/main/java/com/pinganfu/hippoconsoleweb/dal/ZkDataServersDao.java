package com.pinganfu.hippoconsoleweb.dal;

import java.util.List;

import com.pinganfu.hippoconsoleweb.model.ZkDataServersInfoDo;

public interface ZkDataServersDao {

	public List<ZkDataServersInfoDo> selectDataServersList(ZkDataServersInfoDo info)throws Exception;
	
	public int insertDataServersList(ZkDataServersInfoDo info)throws Exception;
	
	public int updateDataServersList(ZkDataServersInfoDo info)throws Exception;
	
	public ZkDataServersInfoDo selectOneDataServers(ZkDataServersInfoDo info)throws Exception;
	
	public int deleteDataServers(ZkDataServersInfoDo info)throws Exception;
}
