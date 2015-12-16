package com.pinganfu.hippoconsoleweb.dal;

import java.util.List;

import com.pinganfu.hippoconsoleweb.model.ZkClusterBackUpInfoDo;

public interface ZkClusterInfoDao {

	public List<ZkClusterBackUpInfoDo> selectBackUpList(ZkClusterBackUpInfoDo info,int rows,int offset)throws Exception;
	
	public Long insertBackUpList(ZkClusterBackUpInfoDo info)throws Exception;
	
	public int updateBackUpList(ZkClusterBackUpInfoDo info)throws Exception;
	
	public ZkClusterBackUpInfoDo selectOneBackUp(ZkClusterBackUpInfoDo info)throws Exception;
	
	public int deleteBackUp(ZkClusterBackUpInfoDo info)throws Exception;
	
	public int selectBackUpCount(ZkClusterBackUpInfoDo info)throws Exception;
	
	public int loadBackupVersion(ZkClusterBackUpInfoDo info)throws Exception;
}
