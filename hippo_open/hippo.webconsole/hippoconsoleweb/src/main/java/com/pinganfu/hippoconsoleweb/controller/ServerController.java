package com.pinganfu.hippoconsoleweb.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.pinganfu.hippoconsoleweb.common.LoadPropertiesData;
import com.pinganfu.hippoconsoleweb.model.ClientModel;
import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.model.ServerModel;
import com.pinganfu.hippoconsoleweb.model.TongModel;
import com.pinganfu.hippoconsoleweb.rule.ChooseServerRuleInterface;
import com.pinganfu.hippoconsoleweb.service.GetJmxInfoInterface;
import com.pinganfu.hippoconsoleweb.service.ServerInfoInterface;
import com.pinganfu.hippoconsoleweb.service.ZkManageInterface;

@Controller
@RequestMapping("server")
public class ServerController {

	@Resource
	private LoadPropertiesData loadPropertiesData;
	@Resource
	private ChooseServerRuleInterface chooseServerRuleInterface;
	@Resource
	private ZkManageInterface zkManageInterface;
	@Resource
	private ServerInfoInterface serverInfoInterface;
	@Resource
	private GetJmxInfoInterface getJmxInfoINterface;
	
	private static Logger logger = LoggerFactory.getLogger(ServerController.class);
	
	@RequestMapping(value = {"/serverList"}, method = RequestMethod.GET)
	public String index(ModelMap model,String name) throws Exception{
        return "/server/server_list";
    }
	
	@RequestMapping(value = {"/dsList"}, method = RequestMethod.GET)
	public String dsList(ModelMap model,String name) throws Exception{
        return "/server/ds_list";
    }
	
	@RequestMapping(value = {"/machineList"}, method = RequestMethod.GET)
	public String machineList(ModelMap model,String name) throws Exception{
        return "/server/server_list";
    }
	
	
	@RequestMapping(value={"/loadServerData"},method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadServerData(ModelMap model,String nowConfiguration)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ServerModel> list = chooseServerRuleInterface.analyseConfiguration(nowConfiguration);
		List<TongModel> tongList = chooseServerRuleInterface.loadTongList(list);
		map.put("tongList", tongList);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/delServerData"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> delServerData(ModelMap model,String serverName,String nowConfiguration)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ServerModel> list = chooseServerRuleInterface.analyseConfiguration(nowConfiguration);
		ServerModel sm = chooseServerRuleInterface.loadServerByName(list, serverName);
		list = chooseServerRuleInterface.delServer(list, sm);
		List<TongModel> tongList = chooseServerRuleInterface.loadTongList(list);
		map.put("tongList", tongList);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/addServerData"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> addServerData(ModelMap model,String serverName,String nowConfiguration)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ServerModel> list = chooseServerRuleInterface.analyseConfiguration(nowConfiguration);
		ServerModel sm = new ServerModel();
		sm.setServerName(serverName);
		list = chooseServerRuleInterface.addServer(list, sm);
		List<TongModel> tongList = chooseServerRuleInterface.loadTongList(list);
		map.put("tongList", tongList);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/changeServerData"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> changeServerData(ModelMap model,String serverName,String nowConfiguration)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ServerModel> list = chooseServerRuleInterface.analyseConfiguration(nowConfiguration);
		list = chooseServerRuleInterface.chooseServer(list, serverName);
		List<TongModel> tongList = chooseServerRuleInterface.loadTongList(list);
		map.put("tongList", tongList);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/loadDsList"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadDsList(ModelMap model,int number,int start,int page,String clusterName,String brokerName,int status,String ip )
			throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		
//		List<ServerInfo> list = zkManageInterface.loadDsList(start,number);
//		int total = zkManageInterface.loadDsListSize();
		
		ServerInfoBean info = new ServerInfoBean();
		info = this.getServerInfoToPage(info,page, number);
		info.setClusterName(clusterName);
		info.setBrokerName(brokerName);
		info.setStatus(status);
		info.setServer_id(ip);
		//info.setIp(ip);
		
		List<ServerInfoBean> list = serverInfoInterface.loadServerInfoList(info);
//		if(list != null && list.size()>0){
//			for(ServerInfoBean bean : list ){
//				bean = searchAndSaveServerInfo(bean, bean.brokerName, bean.brokerVersion);
//			}
//		}
		int total = serverInfoInterface.findServerInfoCount(info);
		map.put("total", total);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/loadServerList"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadServerList(ModelMap model,int size,int page,String clusterName)
			throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		ServerInfoBean info = new ServerInfoBean();
		info = this.getServerInfoToPage1(info,page, size);
		info.setClusterName(clusterName);
		List<ServerInfoBean> list = serverInfoInterface.loadServerInfoList(info);
		if(list != null && list.size()>0){
			for(ServerInfoBean bean : list ){
				bean = searchAndSaveServerInfo(bean, bean.brokerName, bean.brokerVersion);
			}
		}
		int total = serverInfoInterface.findServerInfoCount(info);
		map.put("total", total);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/loadMachineList"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadMachineList(ModelMap model,String clusterName,int status,int page,int size)
			throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		ServerInfoBean info = new ServerInfoBean();
		info = this.getServerInfoToPage1(info,page, size);
		info.setClusterName(clusterName);
		info.setStatus(status);
		List<ServerInfoBean> list = serverInfoInterface.loadServerInfoList(info);
//		if(list != null && list.size()>0){
//			for(ServerInfoBean bean : list ){
//				bean = searchAndSaveServerInfo(bean, bean.brokerName, bean.brokerVersion);
//			}
//		}
		int total = serverInfoInterface.findServerInfoCount(info);
		map.put("total", total);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/loadBucket"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadBucket(ModelMap model,String clusterName,String server_id,String jmxPort,String brokerName)
			throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		
		ServerInfoBean info = zkManageInterface.loadServerBucketInfo(clusterName, server_id,jmxPort,brokerName);
		map.put("brokerName", info.getBrokerName());
		map.put("brokerVersion", info.getBrokerVersion());
		map.put("bucketCount", info.getBucketCount());
		map.put("masterBucket", info.getMasterBucket());
		map.put("slaveBucket", info.getSlaveBucket());
		return map;
	}
	
	@RequestMapping(value={"/findServerInfo"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> findServerInfo(ModelMap model,ServerInfoBean info)
			throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		ServerInfoBean bean = serverInfoInterface.findServerInfo(info);
		String clusterName = bean.getClusterName();
		String server_id = bean.getServer_id();
		ServerInfoBean info1 = zkManageInterface.loadServerBucketInfo(clusterName, server_id,bean.getJmxPort(),bean.getBrokerName());
		bean.setId(bean.getId());
		bean.setSlaveBucket(info1.getSlaveBucket());
		bean.setMasterBucket(info1.getMasterBucket());
		bean.setBucketCount(info1.getBucketCount());
		bean.setMemoryLimit(info1.getMemoryLimit());
		bean.setMemoryPercentUsage(info1.getMemoryPercentUsage());
		bean.setStarted(info1.getStarted());
		bean.setStoreLimit(info1.getStoreLimit());
		bean.setStorePercentUsage(info1.getStorePercentUsage());
		bean.setConn(info1.getConn());
		bean.setClient(info1.getClient());
		bean.setDataDirectory(info1.getDataDirectory());
		bean.setEngineData(info1.getEngineData());
		bean.setEngineName(info1.getEngineName());
		bean.setEngineSize(info1.getEngineSize());
		bean.setCurrentUsedCapacity(info1.getCurrentUsedCapacity());
		//bean.setClient(info1.getClient());
		bean.setJmxPort(info1.getJmxPort());
		map.put("info", bean);
		return map;
	}
	
	
	@RequestMapping(value={"/getConnectionCount"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> getConnectionCount(ModelMap model,ServerInfoBean info,String objectName)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		String[] ipAddress = info.getServer_id().split(":");
		if(ipAddress != null && ipAddress.length>0){
			String ip = ipAddress[0];
			String port = info.getPort();
			if(ipAddress !=null && ipAddress.length>1){
				port = ipAddress[1];
			}
			ServerInfoBean bean = serverInfoInterface.findServerInfo(info);
			
			int connectionCount = getJmxInfoINterface.getConnectionCount(ip,bean.getJmxPort(),objectName);
			map.put("connectionCount", connectionCount);
		}
		
		return map;
	}
	
	@RequestMapping(value={"/getSessionCount"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> getSessionCount(ModelMap model,ServerInfoBean info,String objectName)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		String[] ipAddress = info.getServer_id().split(":");
		if(ipAddress != null && ipAddress.length>0){
			String ip = ipAddress[0];
			String port = info.getPort();
			if(ipAddress !=null && ipAddress.length>1){
				port = ipAddress[1];
			}
			ServerInfoBean bean = serverInfoInterface.findServerInfo(info);
			int sessionCount = getJmxInfoINterface.getSessionCount(ip,bean.getJmxPort(),objectName);
			map.put("sessionCount", sessionCount);
		}
		
		return map;
	}
	
	@RequestMapping(value={"/selectedClient"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> selectedClient(ModelMap model,ServerInfoBean info,String objectName)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		String[] ipAddress = info.getServer_id().split(":");
		if(ipAddress != null && ipAddress.length>0){
			String ip = ipAddress[0];
			String port = info.getPort();
			if(ipAddress !=null && ipAddress.length>1){
				port = ipAddress[1];
			}
			try{
				ServerInfoBean bean = serverInfoInterface.findServerInfo(info);
				List<ClientModel> client =  getJmxInfoINterface.getJmxConnection(ip,bean.getJmxPort(),objectName,bean.getBrokerName());
				map.put("client", client);
			}catch(Exception ex){
				logger.error(" load client is error " + ex.toString());
			}
			
		}
		
		return map;
	}
	
	
	
	@RequestMapping(value={"/saveServer"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> saveServer(ModelMap model,ServerInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		try{
			String server_id = info.getServer_id();
			if(server_id !=null && !server_id.equals("")){
				String[] serArry = server_id.split(":");
				if(serArry !=null && serArry.length>0){
					String ip = serArry[0];
					String port = "";
					if(serArry.length >1){
						port = serArry[1];
					}
					info.setIp(ip);
					info.setPort(port);
				}
			}
			
			ServerInfoBean bean = new ServerInfoBean();
			bean.setDf(0);
			//bean.setClusterId(info.getClusterId());
			//bean.setClusterName(info.getClusterName());
			//bean.setIp(info.getIp());
			//bean.setPort(info.getPort());
			bean.setServer_id(info.getServer_id());
			int count = serverInfoInterface.findServerInfoCount(bean);
			if(count == 0 || info.server_id.equals(info.getOld_server_id())){
				if(!info.getBucketCount().equals("0")){
					info.setStatus(1);
				}else{
					info.setStatus(2);
				}
				int rows = serverInfoInterface.editServerInfo(info);
				map.put("success", "1");
			}else{
				map.put("success", "2");
			}
		}catch(Exception ex){
			logger.error(" insert into server error"+ ex.toString());
			map.put("success", "0");
		}
		
		return map;
	}
	
	@RequestMapping(value={"/addServer"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> addServer(ModelMap model,ServerInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		try{
			String server_id = info.getServer_id();
			if(server_id !=null && !server_id.equals("")){
				String[] serArry = server_id.split(":");
				if(serArry !=null && serArry.length>0){
					String ip = serArry[0];
					String port = "";
					if(serArry.length >1){
						port = serArry[1];
					}
					info.setIp(ip);
					info.setPort(port);
				}
			}
			
			ServerInfoBean bean = new ServerInfoBean();
			bean.setDf(0);
			//bean.setClusterId(info.getClusterId());
			//bean.setClusterName(info.getClusterName());
			//bean.setIp(info.getIp());
			//bean.setPort(info.getPort());
			bean.setServer_id(info.getServer_id());
			int count = serverInfoInterface.findServerInfoCount(bean);
			if(count == 0){
				if(!info.getBucketCount().equals("0")){
					info.setStatus(1);
				}else{
					info.setStatus(2);
				}
				int rows = serverInfoInterface.insertServerInfo(info);
				map.put("success", "1");
			}else{
				map.put("success", "2");
			}
		}catch(Exception ex){
			logger.error(" insert into server error"+ ex.toString());
			map.put("success", "0");
		}
		
		return map;
	}
	
	@RequestMapping(value={"/delServer"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> delServer(ModelMap model,ServerInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		try{
			int rows = serverInfoInterface.delServerInfo(info);
			map.put("success", "1");
		}catch(Exception ex){
			logger.error(" delete server error"+ ex.toString());
			map.put("success", "0");
		}
		return map;
	}
	
	@RequestMapping(value={"/reflashServer"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> reflashServer(ModelMap model,ServerInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		ServerInfoBean bean = serverInfoInterface.findServerInfo(info);
		if(bean != null){
			String clusterName = bean.getClusterName();
			String server_id = bean.getServer_id();
			
			boolean flag = zkManageInterface.pathExists(clusterName, server_id);
			if(flag){
				ServerInfoBean info1 = zkManageInterface.loadServerBucketInfo(clusterName, server_id,bean.getJmxPort(),bean.getBrokerName());
				if(info1 !=null ){
					bean.setSlaveBucket(info1.getSlaveBucket());
					bean.setMasterBucket(info1.getMasterBucket());
					bean.setBucketCount(info1.getBucketCount());
					if(info1.getBrokerName() !=null && !info1.getBrokerName().equals("")){
						bean.setBrokerName(info1.getBrokerName());
					}
					if(info1.getBrokerVersion() !=null && !info1.getBrokerVersion().equals("")){
						bean.setBrokerVersion(info1.getBrokerVersion());
					}
					bean.setStore(info1.getStore());
					bean.setMemory(info1.getMemory());
					bean.setStatus(1);
				}else{
					bean.setSlaveBucket("");
					bean.setMasterBucket("");
					bean.setBucketCount("0");
					bean.setBrokerName(info.getBrokerName());
					bean.setBrokerVersion(info.getBrokerVersion());
					bean.setStore("0");
					bean.setMemory("0");
					bean.setStatus(2);
				}
				try{
					serverInfoInterface.editServerInfo(bean);
				}catch(Exception ex){
					logger.error("edit server info error"+ex.toString());
				}
			}else{
				bean.setStatus(2);
				bean.setSlaveBucket("");
				bean.setMasterBucket("");
				bean.setBucketCount("0");
				try{
					serverInfoInterface.editServerInfo(bean);
				}catch(Exception ex){
					logger.error("edit server info error"+ex.toString());
				}
			}
		}
		map.put("info", bean);
		return map;
	}
	
	
	private ServerInfoBean searchAndSaveServerInfo(ServerInfoBean bean,String brokerName,String borkerVersion)throws Exception{
		if(bean != null){
			String clusterName = bean.getClusterName();
			String server_id = bean.getServer_id();
			
			boolean flag = zkManageInterface.pathExists(clusterName, server_id);
			if(flag){
				ServerInfoBean info1 = zkManageInterface.loadServerBucketInfo(clusterName, server_id,bean.getJmxPort(),brokerName);
				if(info1 !=null ){
					bean.setSlaveBucket(info1.getSlaveBucket());
					bean.setMasterBucket(info1.getMasterBucket());
					bean.setBucketCount(info1.getBucketCount());
					if(info1.getBrokerName() !=null && !info1.getBrokerName().equals("")){
						bean.setBrokerName(info1.getBrokerName());
					}
					if(info1.getBrokerVersion() !=null && !info1.getBrokerVersion().equals("")){
						bean.setBrokerVersion(info1.getBrokerVersion());
					}
					bean.setStore(info1.getStore());
					bean.setMemory(info1.getMemory());
					bean.setStatus(1);
				}else{
					bean.setSlaveBucket("");
					bean.setMasterBucket("");
					bean.setBucketCount("0");
					bean.setBrokerName(brokerName);
					bean.setBrokerVersion(borkerVersion);
					bean.setStore("0");
					bean.setMemory("0");
					bean.setStatus(2);
				}
				try{
					serverInfoInterface.editServerInfo(bean);
				}catch(Exception ex){
					logger.error("edit server info error"+ex.toString());
				}
			}else{
				bean.setStatus(2);
				bean.setSlaveBucket("");
				bean.setMasterBucket("");
				bean.setBucketCount("0");
				try{
					serverInfoInterface.editServerInfo(bean);
				}catch(Exception ex){
					logger.error("edit server info error"+ex.toString());
				}
			}
		}
		return bean;
	}
	
	
	private ServerInfoBean getServerInfoToPage(ServerInfoBean am,int page,int rows){
        int start = (page - 1) *rows;
        am.setOffset(start);
        am.setRows(rows);
        return am;
    }
	
	private ServerInfoBean getServerInfoToPage1(ServerInfoBean setDo,int page,int rows){
		int start = (page - 1) * rows;
		setDo.setOffset(start);
		setDo.setRows(rows);
		return setDo;
    }
	
	
	
}
