package com.hippoconsoleweb.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.annotation.Resource;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.ZkConstants;
import com.hippo.common.domain.HippoClusterConifg;
import com.hippo.common.util.FastjsonUtil;
import com.hippoconsoleweb.common.LoadPropertiesData;
import com.hippoconsoleweb.model.ClusterInfoBean;
import com.hippoconsoleweb.model.ServerInfo;
import com.hippoconsoleweb.model.ServerInfoBean;
import com.hippoconsoleweb.model.ZkManageTree;
import com.hippoconsoleweb.service.ClusterInfoInterface;
import com.hippoconsoleweb.service.ServerInfoInterface;
import com.hippoconsoleweb.service.ZkManageInterface;
import com.hippoconsoleweb.util.ZkUtils;

public class ZkManageInterfaceImpl implements ZkManageInterface {

	private Logger logger = LoggerFactory.getLogger(ZkManageInterfaceImpl.class);
	private String[] NoShowTreeStr = {"hippo"};
	private static String zkAddress = LoadPropertiesData.getZkAddress();
	private ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
	
	
	@Resource
	private ClusterInfoInterface clusterInfoInterface;
	@Resource
	private ServerInfoInterface serverInfoInteface;
	
	
	@Override
	public List<Map<String,Object>> loadZkList(ZkManageTree zkDo)throws Exception {
		List<Map<String,Object>> zkList = new ArrayList<Map<String,Object>>();
		List<String> list = new ArrayList<String>();
		list = zkClient.getChildren("/");
		if(list !=null && list.size()>0){
			List<String> listStr = new ArrayList<String>();
			//--数据过滤
			for(String str : list){
				int c = 0;
				for(String s:NoShowTreeStr){
					if(s.equals(str)){
						c++;
					}
				}
				if(c != 0){
					listStr.add(str);
				}
			}
			if(listStr != null && listStr.size()>0){
				for(String str : listStr){
					Map<String,Object> map = new HashMap<String, Object>();
					String firstStr = str;
					map.put("id", firstStr);
					map.put("text", str);
					List<String> childrenList = zkClient.getChildren("/"+firstStr);
					
					if(childrenList !=null && childrenList.size()>0){
						map.put("state", "closed");
						List<Map<String,Object>> zkSecList = new ArrayList<Map<String,Object>>();
						for(String str2:childrenList){
							Map<String,Object> map2 = new HashMap<String, Object>();
							String secStr = firstStr + "/"+str2;
							map2.put("id", secStr);
							map2.put("text", str2);
							List<String> childrenThirdList = zkClient.getChildren("/"+secStr);
							
							if(childrenThirdList !=null && childrenThirdList.size()>0){
								map2.put("state", "closed");
								List<Map<String,Object>> zkThdList = new ArrayList<Map<String,Object>>();
								for(String str3:childrenThirdList){
									Map<String,Object> map3 = new HashMap<String, Object>();
									String thdStr = secStr + "/"+str3;
									map3.put("id", thdStr);
									map3.put("text", str3);
									
									List<String> childrenFourList = zkClient.getChildren("/"+thdStr);
									if(childrenFourList !=null && childrenFourList.size()>0){
										map3.put("state", "closed");
										List<Map<String,Object>> zkFourList = new ArrayList<Map<String,Object>>();
										for(String str4:childrenFourList){
											Map<String,Object> map4 = new HashMap<String, Object>();
											String fourStr = thdStr + "/"+str4;
											map4.put("id", fourStr);
											map4.put("text", str4);
											map4.put("state", "open");
											zkFourList.add(map4);
										}
										map3.put("children", zkFourList);
									}else{
										map3.put("state", "open");
									}
									zkThdList.add(map3);
								}
								map2.put("children", zkThdList);
							}else{
								map2.put("state", "open");
							}
							zkSecList.add(map2);
						}
						map.put("children", zkSecList);
					}else{
						map.put("state", "open");
					}
					zkList.add(map);
				}
			}
		}
		return zkList;
	}
	
	@Override
	public List<Map<String, Object>> loadZkRootList(ZkManageTree zkDo)throws Exception {
		List<Map<String,Object>> zkList = new ArrayList<Map<String,Object>>();
		List<String> list = new ArrayList<String>();
		if(zkDo.getId() ==null || zkDo.getId().equals("")){
			list = zkClient.getChildren("/");
		}else{
			list = zkClient.getChildren("/"+zkDo.getId());
		}
		
		
		
		if(list != null && list.size()>0){
			for(String text : list){
				int c = 0;
				if(zkDo.getId() ==null || zkDo.getId().equals("")){
					for(String s:NoShowTreeStr){
						if(s.equals(text)){
							c++;
						}
					}
				}else{
					c = 1;
				}
				
				if(c != 0){
					Map<String,Object> map = new HashMap<String, Object>();
					String path = "";
					if(zkDo.getId() ==null || zkDo.getId().equals("")){
						path = "/"+text;
					}else{
						path = "/"+zkDo.getId()+"/"+text;
					}
					String tPath = path.substring(1,path.length());
					map.put("id", tPath);
					map.put("text", text);
					
					if(zkClient.exists(path)){
						List<String> lc1 = zkClient.getChildren(path);
						if(lc1 !=null && lc1.size()>0){
							map.put("state", "closed");
						}else{
							map.put("state", "open");
						}
					}else{
						map.put("state", "open");
					}
					zkList.add(map);
				}
			}
		}
		return zkList;
	}
	
	
	@Override
	public String loadZkRead(ZkManageTree zkDo) throws Exception {
		logger.info(zkDo.getText());
		
		boolean flag = zkClient.exists("/"+zkDo.getText());
		String str =""; 
		if(flag==true){
			return zkClient.readData("/"+zkDo.getText());
		}else{
			return str;
		}
		
	}
	
	@Override
	public boolean ZkWrite(ZkManageTree zkDo) throws Exception {
		
		boolean flag = zkClient.exists("/"+zkDo.getText());
		
		try{
			zkClient.writeData("/"+zkDo.getText(), zkDo.getMemo().trim());
			flag = true;
		}catch(Exception e){
			logger.error("wirte error");
			return false;
		}
		return true;
	}
	
	@Override
	public boolean ZkCreate(ZkManageTree zkDo) throws Exception {
		
		boolean flag = zkClient.exists("/"+zkDo.getId());
		try{
			if(zkDo.getId() !=null && !zkDo.getId().equals("null") && !zkDo.getId().equals("") ){
				zkClient.createPersistent("/"+zkDo.getId()+"/"+zkDo.getText(), zkDo.getMemo().trim());
			}else{
				zkClient.createPersistent("/"+zkDo.getText(), zkDo.getMemo().trim());
			}
			
			flag = true;
		}catch(Exception e){
			logger.error("create error"+e.toString());
			return false;
		}
		return flag;
	}
	
	@Override
	public boolean ZkDelete(ZkManageTree zkDo) throws Exception {
		
		boolean flag = zkClient.exists("/"+zkDo.getId());
		//List<String> childrenList = zkClient.getChildren("/"+zkDo.getId());
		flag = findChildrenToDel(zkDo.getId());
		return flag;
	}
	
	
	public boolean findChildrenToDel(String str)throws Exception{
		
		boolean flag = zkClient.exists("/"+str);
		if(flag){
			List<String> childrenList = zkClient.getChildren("/"+str);
			if(childrenList !=null && childrenList.size()>0){
				for(String str1:childrenList){
					String newPath = str+"/"+str1;
					this.findChildrenToDel(newPath);
				}
			}else{
				try{
					System.out.println("delete:"+str);
					zkClient.delete("/"+str);
					flag = true;
				}catch(Exception ex){
					logger.error("delete error"+ex.toString());
					return false;
				}
			}
			this.findChildrenToDel(str);
		}
		return flag;
	}
	
	
	@Override
	public Map<Integer, Vector<String>> loadTable(String clusterName) throws Exception {
		 Map<Integer, Vector<String>> map = new HashMap<Integer, Vector<String>>();
		 map = serverInfoInteface.loadServerTable(zkAddress,clusterName);
		return map;
	}
	
	@Override
	public List<ServerInfo> loadDsList(int start,int number) throws Exception {
		List<ServerInfo> DsList = new ArrayList<ServerInfo>();
		if(zkClient.exists(ZkConstants.DATA_SERVERS)){
			List<String> list = zkClient.getChildren(ZkConstants.DATA_SERVERS);
			if(list !=null && list.size()>0){
				int num = 0;
				int i = 0;
				for(String str : list){
					if(zkClient.exists(ZkConstants.DATA_SERVERS + "/"+str)){
						String json = zkClient.readData(ZkConstants.DATA_SERVERS + "/"+str);
						ServerInfo info = (ServerInfo)FastjsonUtil.jsonToObj(json, ServerInfo.class);
						if(info !=null && info.getServer_id() != null){
							if(i >= start){
								ServerInfo foo = new ServerInfo();
								foo.setServer_id(info.getServer_id());
								foo.setMemory(String.valueOf(this.rand()));
								foo.setStore(String.valueOf(this.rand()));
								DsList.add(foo);
								num ++ ;
							}
							i++;
						}
						if(num == number){
							break;
						}
					}
					
				}
			}
		}
		return DsList;
	}
	
	
	public int rand(){
	    int s = (int)(Math.random()*10*9);
	    return s;
	}
	
	@Override
	public int loadDsListSize() throws Exception {
		int count = 0;
		if(zkClient.exists(ZkConstants.DATA_SERVERS)){
			List<String> list = zkClient.getChildren(ZkConstants.DATA_SERVERS);
			count = list.size();
		}
		return count;
	}
	
	@Override
	public List<Map<String, String>> loadCluseterMenuZk() throws Exception {
		
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = new HashMap<String, String>();
		map.put("text", "--请选择--");
		map.put("id", "");
		list.add(map);
		
//		List<ClusterInfoBean> infoList = new ArrayList<ClusterInfoBean>();
//		ClusterInfoBean info = new ClusterInfoBean();
//		info.setDf(0);
//		infoList = clusterInfoInterface.loadClusterInfoList(info);
		
		String path = ZkConstants.DEFAULT_PATH_ROOT;
		if(zkClient.exists(path)){
			List<String> clusterList = zkClient.getChildren(path);
			if(clusterList != null && clusterList.size()>0){
				for(String clusterName : clusterList){
					int c = 0;
//					if(infoList !=null && infoList.size()>0){
//						for(ClusterInfoBean bean:infoList){
//							if(clusterName.equals(bean.getClusterName()))
//								c++;
//						}
//					}
					if(c==0){
						map = new HashMap<String, String>();
						map.put("text", clusterName);
						map.put("id", clusterName);
						list.add(map);
					}
				}
			}
		}
		return list;
	}
	
	@Override
	public List<Map<String, String>> loadCluseterMenuBase() throws Exception {
		
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = new HashMap<String, String>();
		map.put("text", "--请选择--");
		map.put("id", "");
		list.add(map);
		
		List<ClusterInfoBean> infoList = new ArrayList<ClusterInfoBean>();
		ClusterInfoBean info = new ClusterInfoBean();
		info.setDf(0);
		infoList = clusterInfoInterface.loadClusterInfoList(info);
		
		if(list !=null && list.size()>0){
			for(ClusterInfoBean bean : infoList){
				map = new HashMap<String, String>();
				map.put("text", bean.getClusterName());
				map.put("id", String.valueOf(bean.getId()));
				list.add(map);
			}
		}
		return list;
	}
	
	@Override
	public List<Map<String, String>> loadCluseterMenuBaseSelected()
			throws Exception {
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = new HashMap<String, String>();
//		map.put("text", "--请选择--");
//		map.put("id", "");
//		list.add(map);
		
		List<ClusterInfoBean> infoList = new ArrayList<ClusterInfoBean>();
		ClusterInfoBean info = new ClusterInfoBean();
		info.setDf(0);
		infoList = clusterInfoInterface.loadClusterInfoList(info);
		
		if(infoList !=null && infoList.size()>0){
			int i =0;
			for(ClusterInfoBean bean : infoList){
				map = new HashMap<String, String>();
				map.put("name", bean.getClusterName());
				map.put("id", String.valueOf(bean.getId()));
				if(i == 0){
					map.put("selected","true");
				}else{
					map.put("selected", "false");
				}
				i++;
				list.add(map);
			}
		}
		return list;
	}
	
	@Override
	public List<Map<String, String>> loadServerMenuZk(String clusterName) throws Exception {
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = new HashMap<String, String>();
//		map.put("text", "--请选择--");
//		map.put("id", "");
//		list.add(map);
		
		List<ServerInfoBean> serList = serverInfoInteface.loadServerInfoByZk(clusterName, zkAddress);
		if(serList !=null && serList.size()>0){
			for(ServerInfoBean ser : serList){
				map = new HashMap<String, String>();
				map.put("name", ser.getServer_id());
				map.put("id", ser.getServer_id());
				list.add(map);
			}
		}
		return list;
	}
	
	@Override
	public ServerInfoBean loadServerBucketInfo(String clusterName, String server_id,String jmxPort,String brokerName)throws Exception {
		ServerInfoBean info = new ServerInfoBean();
		info.setServer_id(server_id);
		info.setJmxPort(jmxPort);
		info.setBrokerName(brokerName);
		info = serverInfoInteface.loadBucketCount(clusterName, info, zkAddress);
			try{
				info = serverInfoInteface.getJmxValue(info);
			}catch(Exception ex){
				logger.error("load jmx error"+ex.toString() );
			}
		
		return info;
	}
	
	@Override
	public boolean pathExists(String clusterName, String server_id)throws Exception {
		String path = ZkConstants.DEFAULT_PATH_ROOT + "/" +clusterName + ZkConstants.NODE_DATA_SERVERS;

		boolean flag = zkClient.exists(path);
		if(flag){
			List<String> childen = zkClient.getChildren(path);
			if(childen !=null && childen.size()>0){
				int i = 0;
				for(String str : childen){
//					String childenPath = path + "/"+str;
//					if(zkClient.exists(childenPath)){
//						String json = zkClient.readData(childenPath);
//						ServerInfo info = (ServerInfo) FastjsonUtil.jsonToObj(json, ServerInfo.class);
//						if(info.getServer_id().equals(server_id)){
//							i++;
//						}
//					}else{
//						return false;
//					}
					if(server_id.equals(str)){
						i++;
					}
				}
				if(i > 0){
					return true;
				}else{
					return false;
				}
			}else{
				return false;
			}
		}
		return flag;
	}
	
	@Override
	public String loadDsChangeType() throws Exception {
		String path = "";//ZkConstants.DS_CHANGE_TYPE ;
		String type = "";
		boolean flag = zkClient.exists(path);
		if(flag){
			type = zkClient.readData(path);
		}
		return type;
	}
	
	@Override
	public boolean sendClusterToZk(ClusterInfoBean info) throws Exception {
		HippoClusterConifg config = new HippoClusterConifg();
		config.setCopycount(info.getCopyCount());
		config.setDbType(info.getDbType());
		config.setBucketsLimit(String.valueOf(info.getBucketLimit()));
		config.setName(info.getClusterName());
		config.setHashcount(info.getHashCount());
		config.setReplicatePort(info.getReplicatePort());
		String configJson = (String)FastjsonUtil.objToJson(config);
		String clusterPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+info.getClusterName();

		String configPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+info.getClusterName() + ZkConstants.NODE_CONFIG;
		String dataserversPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+info.getClusterName()+ZkConstants.NODE_DATA_SERVERS;
		String tablePath = ZkConstants.DEFAULT_PATH_ROOT + "/"+info.getClusterName()+ZkConstants.NODE_TABLES;
		String ctPath = tablePath+ZkConstants.NODE_CTABLE;
		String mtPath = tablePath+ZkConstants.NODE_MTABLE;
		String dtPath = tablePath+ZkConstants.NODE_DTABLE;
		String migrationPath = clusterPath +"/migration";
		try{
			if(!zkClient.exists(clusterPath)){
				zkClient.createPersistent(clusterPath);
			}
			if(!zkClient.exists(configPath)){
				zkClient.createPersistent(configPath);
			}
			if(!zkClient.exists(dataserversPath)){
				zkClient.createPersistent(dataserversPath);
			}
			if(!zkClient.exists(tablePath)){
				zkClient.createPersistent(tablePath);
			}
			if(!zkClient.exists(ctPath)){
				zkClient.createPersistent(ctPath);
			}
			if(!zkClient.exists(mtPath)){
				zkClient.createPersistent(mtPath);
			}
			if(!zkClient.exists(dtPath)){
				zkClient.createPersistent(dtPath);
			}
			if(!zkClient.exists(migrationPath)){
				zkClient.createPersistent(migrationPath);
			}
			zkClient.writeData(configPath, configJson);
			clusterInfoInterface.addSubscribeClidChangs(info.getClusterName(), dataserversPath,zkAddress);
		}catch(Exception ex){
			logger.error("set zk error:"+ex.toString());
			return false;
		}
		return true;
	}
	
	@Override
	public boolean zkDataRest(String clusterName) throws Exception {
		String migrationPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+clusterName +ZkConstants.NODE_MIGRATION;
		if(zkClient.exists(migrationPath)){
			List<String> migrationList = (List<String>)zkClient.getChildren(migrationPath);
			if(migrationList !=null && migrationList.size()>0){
				for(String migrationStr:migrationList){
					String deletePath = migrationPath + "/" +migrationStr;
					if(zkClient.exists(deletePath)){
						zkClient.delete(deletePath);
					}else{
						return false;
					}
				}
				
			}
		}
		String mPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+clusterName +ZkConstants.NODE_TABLES + ZkConstants.NODE_MTABLE;
		if(zkClient.exists(mPath)){
			zkClient.writeData(mPath, "null");
		}else{
			return false;
		}
		String dPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+clusterName +ZkConstants.NODE_TABLES + ZkConstants.NODE_DTABLE;
		if(zkClient.exists(dPath)){
			zkClient.writeData(dPath, "null");
		}else{
			return false;
		}
		String cPath = ZkConstants.DEFAULT_PATH_ROOT + "/"+clusterName +ZkConstants.NODE_TABLES + ZkConstants.NODE_CTABLE;
		if(zkClient.exists(cPath)){
			zkClient.writeData(cPath, "null");
		}else{
			return false;
		}
		
		return true;
	}
	

}
