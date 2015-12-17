package com.hippoconsoleweb.dal;

import java.util.List;

import com.hippoconsoleweb.model.ServerInfoDo;

public interface ServerInfoDao {

	public List<ServerInfoDo> loadServerInfoList(ServerInfoDo info)throws Exception;
	
	public int insertServerInfo(ServerInfoDo info)throws Exception;
	
	public ServerInfoDo findServerInfo(ServerInfoDo info)throws Exception;
	
	public int findServerInfoCount(ServerInfoDo info)throws Exception;
	
	public int delServerInfo(ServerInfoDo info)throws Exception;
	
	public int editServerInfo(ServerInfoDo info)throws Exception;
}
