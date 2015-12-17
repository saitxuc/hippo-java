package com.hippoconsoleweb.dal;

import java.util.List;

import com.hippoconsoleweb.model.ClusterInfoDo;

public interface ClusterInfoDao{

	public List<ClusterInfoDo> loadClusterInfoList(ClusterInfoDo info)throws Exception;
	
	public int insertClusterInfo(ClusterInfoDo info)throws Exception;
	
	public ClusterInfoDo findClusterInfo(ClusterInfoDo info)throws Exception;
	
	public int findClusterInfoCount(ClusterInfoDo info)throws Exception;
	
	public int delClusterInfo(ClusterInfoDo info)throws Exception;
	
	public int editClusterInfo(ClusterInfoDo info)throws Exception;
	
}
