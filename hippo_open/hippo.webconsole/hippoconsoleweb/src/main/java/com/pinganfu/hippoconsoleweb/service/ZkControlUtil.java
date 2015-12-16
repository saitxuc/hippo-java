package com.pinganfu.hippoconsoleweb.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.ZkConstants;
import com.pinganfu.hippo.common.domain.HippoClusterConifg;
import com.pinganfu.hippo.common.domain.HippoClusterTableInfo;
import com.pinganfu.hippo.common.util.FastjsonUtil;
import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.tablebuilder.LbFirstTableBuilder;

public class ZkControlUtil {
	
	private static Logger logger = LoggerFactory.getLogger(ZkControlUtil.class);
	
	private ZkControlUtil() {
	}
	
	/**
	 * 获取ServerInfoBean
	 * @param zkClient
	 * @param dataServer
	 * @return
	 * @throws Exception
	 */
	/*
	public static  ServerInfoBean loadDataServerName(ZkClient zkClient,String dataServer)throws Exception{
		ServerInfoBean ServerInfoBean = new ServerInfoBean();
		String serverPath = ZkConstants.DATA_SERVERS + "/" + dataServer;
		if(zkClient.exists(serverPath)){
			String zkData = zkClient.readData(serverPath);
			ServerInfoBean = (ServerInfoBean)FastjsonUtil.jsonToObj(zkData, ServerInfoBean.class);
		}
		return ServerInfoBean;
	}*/
	
	/**
	 * 获取server的信息
	 * @param zkClient
	 * @param clusterName
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public static Map<Integer, Vector<String>> loadServerTable(ZkClient zkClient,String clusterName,String tableName)throws Exception{
		Map<Integer, Vector<String>> map = new HashMap<Integer, Vector<String>>();
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + "/"+tableName ;
		if(zkClient.exists(path)){
			String zkData = zkClient.readData(path);
			HippoClusterTableInfo hippoCluster = (HippoClusterTableInfo)FastjsonUtil.jsonToObj(zkData,HippoClusterTableInfo.class);
			if(hippoCluster !=null){
				map = hippoCluster.getTableMap();
				return map;
			}
		}
		return null;
	}
	
	public static Map<Integer, Vector<String>> loadTableFastRows(ZkClient zkClient,String clusterName,String tableName,Map<Integer, Vector<String>> hash_table_for_builder_tmp)throws Exception{
		Map<Integer, Vector<String>> map = hash_table_for_builder_tmp;
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + "/"+tableName ;
		if(zkClient.exists(path)){
			String zkData = zkClient.readData(path);
			HippoClusterTableInfo hippoCluster = (HippoClusterTableInfo)FastjsonUtil.jsonToObj(zkData,HippoClusterTableInfo.class);
			if(hippoCluster !=null){
				map.put(0, hippoCluster.getTableMap().get(0)) ;
				return map;
			}
		}
		return null;
	}

	/**
	 * 将修改后的表写入到zk
	 * @param zkClient
	 * @throws Exception
	 */
	public static void rebuildTable(ZkClient zkClient, int up, int down)throws Exception{
		//--查找出默认路径下有多少个集群
		String path = ZkConstants.DEFAULT_PATH_ROOT;
		if(zkClient.exists(path)){
			List<String> clusterList = zkClient.getChildren(path);
			if(clusterList !=null && clusterList.size()>0){
				for(String clusterName : clusterList){
					/// if(isMigrateComplete(zkClient, clusterName)){		
						//--load dtable
						String dtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES+ZkConstants.NODE_DTABLE;
						HippoClusterTableInfo oldDtable = (HippoClusterTableInfo) FastjsonUtil.jsonToObj(zkClient.readData(dtablePath).toString(), HippoClusterTableInfo.class);

						//--create new Dtable
						Map<Integer, Vector<String>> newDtable = rebuildAndWriteDtableData(zkClient, clusterName);
						
                        // DPJ: write dtable after mtable been built, because old dtable is needed to build mtable
                        if(newDtable !=null){
                            String clusterPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES+ZkConstants.NODE_DTABLE;
                            if(zkClient.exists(clusterPath)){
                                writeTableData(clusterName, ConsoleConstants.DTABLE, zkClient, newDtable, false);
                            }
                        }
                        
						//--写入Mtable 和 Ctable
                        if(newDtable != null) {
                        	int copyCnt = newDtable.size();
                        	int bucketCount = newDtable.get(0).size();
                        	if(oldDtable == null) {
                        		writeMtableDataAndCtableData(ZkControlUtil.buildEmptyTable(bucketCount, copyCnt), zkClient, clusterName, up, down);
                        	} else {
                        		writeMtableDataAndCtableData(oldDtable.getTableMap(), zkClient, clusterName, up, down);
                        	}
                        }

					/// }
				}
			}
		}
	}
	
	/**
	 * 返回Zk中的参数
	 * @param clusterName
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	public static List<ServerInfoBean> loadServerInfoBeanByZk(String clusterName,ZkClient zkClient)throws Exception{
		List<ServerInfoBean> list = new ArrayList<ServerInfoBean>();
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_DATA_SERVERS;
		if(zkClient.exists(path)){
			List<String> dsList = zkClient.getChildren(path);
			if(dsList !=null && dsList.size()>0){
				for(String ds : dsList){
					String dsPath = path + "/"+ds;
					if(zkClient.exists(dsPath)){
						String dsData = zkClient.readData(dsPath);
						ServerInfoBean ServerInfoBean = (ServerInfoBean)FastjsonUtil.jsonToObj(dsData, ServerInfoBean.class);
						list.add(ServerInfoBean);
					}
				}
			}
		}
		return list;
	}
	
	/**
	 * 获取BucketCount
	 * @param clusterName
	 * @param info
	 * @return
	 * @throws Exception
	 */
	public static ServerInfoBean loadBucketCount (String clusterName, ServerInfoBean info,ZkClient zkClient)throws Exception{
		Map<Integer, Vector<String>> hashTable = loadServerTable(zkClient,clusterName,ConsoleConstants.MTABLE);
		int bucketCount = 0;
		String masterBucket = "";
		String slaveBucket = "";
		if(hashTable != null && hashTable.size()>0){
			for(int i=0;i<hashTable.size();i++){
				Vector<String> vt = (Vector<String>) hashTable.get(i);
				if(vt !=null && vt.size()>0){
					int index = 0;
					for(String ser : vt){
						if(ser.equals(info.getServer_id())){
							bucketCount++;
							if(i==0){
								masterBucket = masterBucket + index + ",";
							}else{
								slaveBucket = slaveBucket + index + ",";
							}
						}
						index++;
					}
				}
			}
			if(masterBucket !=null && masterBucket.length() > 0){
				masterBucket = masterBucket.substring(0,masterBucket.length()-1);
			}
			if(slaveBucket !=null && slaveBucket.length() > 0){
				slaveBucket = slaveBucket.substring(0,slaveBucket.length()-1);
			}
		}
		info.setMasterBucket(masterBucket);
		info.setSlaveBucket(slaveBucket);
		info.setBucketCount(String.valueOf(bucketCount));
		
		return info;
	}
	
	
	
	
	/**
	 * 返回DataServers
	 * @return
	 */
	public static List<ServerInfoBean> getDataServers(){
		return getDataServers();
	}
	
	/**
	 * =========================================================对内私有方法======================================================
	 */
	
	/**
	 * 返回当前的服务器列表
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	private static List<ServerInfoBean> getDataServers(ZkClient zkClient,String clusterName)throws Exception{
		List<ServerInfoBean> list = new ArrayList<ServerInfoBean>();
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_DATA_SERVERS;
		if(zkClient.exists(path)){
			List<String> dataServers = zkClient.getChildren(path);
			if(dataServers != null && dataServers.size()>0){
				for(String server : dataServers){
					ServerInfoBean ser = new ServerInfoBean();
					ser.setServer_id(server);
					list.add(ser);
				}
			}
		}
		return list;
	}
	
	/**
	 * 算法计算
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	private static Map<Integer, Vector<String>> resetTable(ZkClient zkClient,String clusterName)throws Exception{
		HippoClusterConifg config = getClusterConfig(zkClient, clusterName);
		if(config !=null){
			int bucketCount = config.getHashcount();
			int coypCount = config.getCopycount();
			List<ServerInfoBean> list = getDataServers(zkClient,clusterName);
			Map<Integer, Vector<String>> hash_table_for_builder_tmp = loadServerTable(zkClient,clusterName,ConsoleConstants.MTABLE);
			if(hash_table_for_builder_tmp == null ){
				hash_table_for_builder_tmp = buildEmptyTable(bucketCount, coypCount);
				/// writeTableData(clusterName, ConsoleConstants.MTABLE, zkClient, hash_table_for_builder_tmp);
			}
			
			LbFirstTableBuilder p_table_builder = new LbFirstTableBuilder(bucketCount,coypCount); 
			Map<Integer, Vector<String>> hash_table_result = new HashMap<Integer, Vector<String>>();
	        Set<ServerInfoBean> ava_server = new HashSet<ServerInfoBean>();
	        if(list !=null && list.size()>0){
	        	for(ServerInfoBean ser:list){
	        		ServerInfoBean node = new ServerInfoBean();
	        		node.setServer_id(ser.getServer_id());
	        		ava_server.add(node);
	        	}
	        	p_table_builder.set_available_server(ava_server);
	            
	            int result = p_table_builder.rebuild_table(hash_table_for_builder_tmp, hash_table_result, true);
	            if(ConsoleConstants.BUILD_OK == result) {
	        		return hash_table_result;   	
	            }
	        }
		}
			
    	return null;
	}
	
	/**
	 * getmTable
	 * @param zkClient
	 * @param clusterName
	 * @return
	 * @throws Exception
	 */
	private static Map<Integer, Vector<String>> buildMtable(ZkClient zkClient,String clusterName,Map<Integer, Vector<String>> newDtable)throws Exception{
		HippoClusterConifg config = getClusterConfig(zkClient, clusterName);
		Map<Integer, Vector<String>> newMtable = null;
		if(config !=null){
			int bucketCount = config.getHashcount();
			int coypCount = config.getCopycount();
			List<ServerInfoBean> list = getDataServers(zkClient,clusterName);
			Map<Integer, Vector<String>> hash_table_for_builder_tmp = loadServerTable(zkClient,clusterName,ConsoleConstants.MTABLE);
			if(hash_table_for_builder_tmp == null ){
				//return null;
				hash_table_for_builder_tmp = buildEmptyTable(bucketCount, coypCount);
				hash_table_for_builder_tmp = loadTableFastRows(zkClient,clusterName,ConsoleConstants.DTABLE,hash_table_for_builder_tmp);
			}
			LbFirstTableBuilder p_table_builder = new LbFirstTableBuilder(bucketCount,coypCount); 
	        Set<ServerInfoBean> ava_server = new HashSet<ServerInfoBean>();
	        if(list !=null && list.size()>0){
	        	for(ServerInfoBean ser:list){
	        		ServerInfoBean node = new ServerInfoBean();
	        		node.setServer_id(ser.getServer_id());
	        		ava_server.add(node);
	        	}
	        	p_table_builder.set_available_server(ava_server);
	            
	        	newMtable  = p_table_builder.buildQuickTable(newDtable,hash_table_for_builder_tmp);
	        	
	        }
		}
		
		return newMtable;
	} 
	
	/**
	 * add by DPJ
	 * @param zkClient
	 * @param clusterName
	 * @param oldDtable
	 * @return
	 * @throws Exception
	 */
	private static Map<Integer, Vector<String>> buildMtable2(ZkClient zkClient,String clusterName,Map<Integer, Vector<String>> oldDtable)throws Exception{
		HippoClusterConifg config = getClusterConfig(zkClient, clusterName);
		Map<Integer, Vector<String>> newMtable = null;
		if(config !=null){
			int bucketCount = config.getHashcount();
			int coypCount = config.getCopycount();
			List<ServerInfoBean> list = getDataServers(zkClient,clusterName);
			Map<Integer, Vector<String>> hash_table_for_builder_tmp = loadServerTable(zkClient,clusterName,ConsoleConstants.MTABLE);
			if(hash_table_for_builder_tmp == null ){
				//return null;
				hash_table_for_builder_tmp = buildEmptyTable(bucketCount, coypCount);
				hash_table_for_builder_tmp = loadTableFastRows(zkClient,clusterName,ConsoleConstants.DTABLE,hash_table_for_builder_tmp);
				/// DPJ
				return hash_table_for_builder_tmp;
				
			}
			LbFirstTableBuilder p_table_builder = new LbFirstTableBuilder(bucketCount,coypCount); 
	        Set<ServerInfoBean> ava_server = new HashSet<ServerInfoBean>();
	        if(list !=null && list.size()>0){
	        	for(ServerInfoBean ser:list){
	        		ServerInfoBean node = new ServerInfoBean();
	        		node.setServer_id(ser.getServer_id());
	        		ava_server.add(node);
	        	}
	        	p_table_builder.set_available_server(ava_server);
	            
	        	newMtable = p_table_builder.build_quick_table_alone(oldDtable); 
	        	
	        }
		}
		
		return newMtable;
	} 
	
	/**
	 * 获取config信息
	 * @param zkClient
	 * @param clusterName
	 * @return
	 * @throws Exception
	 */
	private static HippoClusterConifg getClusterConfig(ZkClient zkClient,String clusterName)throws Exception{
		HippoClusterConifg config = new HippoClusterConifg();
		String configPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName+ZkConstants.NODE_CONFIG;
		
		if(zkClient.exists(configPath)){
			config = com.pinganfu.hippo.common.util.FastjsonUtil.jsonToObj((String)zkClient.readData(configPath), HippoClusterConifg.class);
		}
		return config;
	}
	
	/**
	 * 写入Dtable
	 * @param hash_table_result
	 * @return
	 * @throws Exception
	 */
	private static Map<Integer, Vector<String>> rebuildAndWriteDtableData(ZkClient zkClient,String clusterName)throws Exception{
		Map<Integer, Vector<String>> hash_table_result = null ;
		try{
			hash_table_result = resetTable(zkClient,clusterName); //--获取数据
		}catch(Exception ex){
			logger.error("reset table error :"+ex.toString());
		}
		return hash_table_result;
	}
	
	/**
	 * 写入Mtable 和 Ctable
	 * @param hash_table_result
	 * @throws Exception
	 */
	private static void writeMtableDataAndCtableData(Map<Integer, Vector<String>> oldDtable,ZkClient zkClient,String clusterName, int up, int down)throws Exception{
		try{
			Map<Integer, Vector<String>> mtable_result = buildMtable2(zkClient, clusterName,oldDtable); //--获取数据
			System.out.println("=============:" + mtable_result);
			String mtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES+ZkConstants.NODE_MTABLE;
			String ctablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES+ZkConstants.NODE_CTABLE;
			if(mtable_result != null){
				if(zkClient.exists(mtablePath)){
					writeTableData(clusterName, ConsoleConstants.MTABLE, zkClient, mtable_result, true);
				}
				if(down > 0 && zkClient.exists(ctablePath)){
					writeTableData(clusterName, ConsoleConstants.CTABLE, zkClient, mtable_result, true);
				}
			}
			/*
			else{
				if(hash_table_result != null){
					if(zkClient.exists(mtablePath)){
						writeTableData(clusterName, ConsoleConstants.MTABLE, zkClient, mtablePath, hash_table_result);
					}
					if(zkClient.exists(ctablePath)){
						writeTableData(clusterName, ConsoleConstants.CTABLE, zkClient, ctablePath, hash_table_result);
					}
				}
			}*/
		}catch(Exception ex){
			logger.error("Get mTable error:"+ex.toString());
		}
	}
	
	/**
	 * 写入数据操作
	 * @param clusterName
	 * @param tableName
	 * @param zkClient
	 * @param tablePath
	 * @param tableMap
	 * @throws Exception
	 */
	private static void writeTableData(String clusterName,String tableName,ZkClient zkClient,Map<Integer, Vector<String>> tableMap, boolean incVersion)throws Exception{
        String tablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + "/" + tableName;
        HippoClusterTableInfo hippoCluster = new HippoClusterTableInfo();
        hippoCluster.setTableMap(tableMap);
        if(incVersion) {
            int version = getNextTableVersion(zkClient, clusterName, tableName);
            hippoCluster.setVersion(version);
        }
        zkClient.writeData(tablePath, FastjsonUtil.objToJson(hippoCluster));
    }
	
	/**
	 * 获取table的版本
	 * @param zkClient
	 * @param clusterName
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	private static int getNextTableVersion(ZkClient zkClient,String clusterName,String tableName)throws Exception{
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + "/"+tableName ;
		if(zkClient.exists(path)){
			String zkData = zkClient.readData(path);
			HippoClusterTableInfo hippoCluster = (HippoClusterTableInfo)FastjsonUtil.jsonToObj(zkData, HippoClusterTableInfo.class);
			if(hippoCluster != null){
				int version = hippoCluster.getVersion();
				version++;
				return version;
			}
		}
		return 1;
	}
	
	
	
	/**
	 * @return
	 * @throws Exception
	 */
	private static boolean isMigrateComplete(ZkClient zkClient ,String cluster)throws Exception{
		String dTablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_TABLES+ZkConstants.NODE_DTABLE;
		String mTablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_TABLES+ZkConstants.NODE_MTABLE;
		
		
		
		if(zkClient.exists(dTablePath) && zkClient.exists(mTablePath)){
			String dTableData = getTableDateByPath(dTablePath, zkClient);
			String mTableData = getTableDateByPath(mTablePath, zkClient);
			if(dTableData.trim().equals(mTableData.trim())){
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * 根据zk路径获取相关的table中的信息
	 * @param tablePath
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	public static String getTableDateByPath(String tablePath,ZkClient zkClient)throws Exception{
		String tableData = zkClient.readData(tablePath);
		String mapJson = "";
		if(FastjsonUtil.jsonToObj(tableData, HippoClusterTableInfo.class) !=null){
			HippoClusterTableInfo tableInfo = (HippoClusterTableInfo)FastjsonUtil.jsonToObj(tableData, HippoClusterTableInfo.class);
			Map<Integer, Vector<String>> tableMap = tableInfo.getTableMap();
			mapJson = (String)FastjsonUtil.objToJson(tableMap);
		}
		return mapJson;
	}
	
	public static HippoClusterTableInfo getTableInfoByPath(String tablePath,ZkClient zkClient)throws Exception{
        String tableData = zkClient.readData(tablePath);
        return (HippoClusterTableInfo)FastjsonUtil.jsonToObj(tableData, HippoClusterTableInfo.class);
    }
	
	/**
	 *  初始化hash
	 * @param bucketCount
	 * @param coypCount
	 * @return
	 * @throws Exception
	 */
	public static Map<Integer, Vector<String>> buildEmptyTable(int bucketCount , int coypCount)throws Exception{
		Map<Integer, Vector<String>> map = new HashMap<Integer, Vector<String>>();
		for(int i=0;i<coypCount;i++){
			Vector<String> nodeV = new Vector<String>();
			for(int j=0;j<bucketCount;j++){
				nodeV.add("0");
			}
			map.put(i, nodeV);
		}
		return map;
	}
	

}
