package com.hippoconsoleweb.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

import com.hippo.common.ZkConstants;
import com.hippoconsoleweb.model.ZkClusterBackUpInfoBean;
import com.hippoconsoleweb.model.ZkDataServersInfoBean;
import com.hippoconsoleweb.model.ZkTablesInfoBean;
import com.hippoconsoleweb.service.BackupService;

public class ZkBackUtil {
	
	/**
	 * zk backup
	 * @param zkAddress
	 * @throws Exception
	 */
	public static void zkBackUp(String zkAddress,BackupService backupService)throws Exception{
		ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
		
		String path = ZkConstants.DEFAULT_PATH_ROOT;
		
		if(zkClient.exists(path)){
			List<String> subList = zkClient.getChildren(path);
			if(subList != null && subList.size()>0){
				for(String cluster : subList ){
                    ZkClusterBackUpInfoBean  bean = getZkClusterBuckUpByClusterName(cluster, zkClient,backupService);
                    backupService.save(bean);
				}
			}
		}
	}
	
	/**
	 * get cluster value
	 * @param cluster
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	private static ZkClusterBackUpInfoBean getZkClusterBuckUpByClusterName(String cluster,ZkClient zkClient,BackupService backupService)throws Exception{
		ZkClusterBackUpInfoBean bean = new ZkClusterBackUpInfoBean();
		
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_MIGRATION;
		String configPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_CONFIG;
		
		String migrationStr = "";
		if(zkClient.exists(path)){
			List<String> migrationList = zkClient.getChildren(path);
			if(migrationList !=null && migrationList.size()>0){
				for(String migration : migrationList){
					migrationStr = migrationStr + migration +",";
				}
			}
			if(migrationStr != null && migrationStr.length()>0){
				migrationStr = migrationStr.substring(0,migrationStr.length()-1);
				bean.setMigration(migrationStr);
			}
		}
		
		if(zkClient.exists(configPath)){
			String config = zkClient.readData(configPath);
			bean.setConfig(config);
		}
		
		bean.setClusterName(cluster);
		bean.setVersion(backupService.getVersion(cluster));
		bean.setCreatedate(new Date());
		bean.setDf(0);
		bean.setDataservers(getZkDataServersByClusterName(cluster, zkClient));
		bean.setTables(getZkTablesInfoByClusterName(cluster, zkClient));
		return bean;
	}
	
	/**
	 * get dataservers value
	 * @param cluster
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	private static List<ZkDataServersInfoBean> getZkDataServersByClusterName(String cluster,ZkClient zkClient)throws Exception{
		List<ZkDataServersInfoBean> list = new ArrayList<ZkDataServersInfoBean>();
		
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_DATA_SERVERS;
		
		if(zkClient.exists(path)){
			List<String> serverList = zkClient.getChildren(path);
			if(serverList !=null && serverList.size()>0){
				for(String server : serverList){
					ZkDataServersInfoBean bean = new ZkDataServersInfoBean();
					bean.setCreatedate(new Date());
					bean.setDf(0);
					bean.setNetworkPort(server);
					bean.setContent("");
					list.add(bean);
				}
			}
		}
		
		return list;
	}
	
	/**
	 * get tables list value
	 * @param cluster
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	private static List<ZkTablesInfoBean> getZkTablesInfoByClusterName(String cluster,ZkClient zkClient)throws Exception{
		
		List<ZkTablesInfoBean> list = new ArrayList<ZkTablesInfoBean>();
		
		String mTpath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_TABLES + ZkConstants.NODE_MTABLE;
		String cTpath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_TABLES + ZkConstants.NODE_CTABLE;
		String dTpath = ZkConstants.DEFAULT_PATH_ROOT + "/" + cluster + ZkConstants.NODE_TABLES + ZkConstants.NODE_DTABLE;
		
		if(zkClient.exists(mTpath)){
			String mtable = zkClient.readData(mTpath);
			ZkTablesInfoBean bean = setTablesBean(mtable,0,"mtable");
			list.add(bean);
		}
		if(zkClient.exists(cTpath)){
			String ctable = zkClient.readData(cTpath);
			ZkTablesInfoBean bean = setTablesBean(ctable,0,"ctable");
			list.add(bean);
		}
		if(zkClient.exists(dTpath)){
			String dtable = zkClient.readData(dTpath);
			ZkTablesInfoBean bean = setTablesBean(dtable,0,"dtable");
			list.add(bean);
		}
		
		return list;
	}
	
	/**
	 * set tables 
	 * @param content 
	 * @param df
	 * @param type ('mtable','dtable','ctable')
	 * @throws Exception
	 */
	private static ZkTablesInfoBean setTablesBean(String content,int df,String type)throws Exception{
		ZkTablesInfoBean bean = new ZkTablesInfoBean();
		bean.setContent(content);
		bean.setDf(0);
		bean.setCreatedate(new Date());
		bean.setType(type);
		return bean;
	}
	

}
