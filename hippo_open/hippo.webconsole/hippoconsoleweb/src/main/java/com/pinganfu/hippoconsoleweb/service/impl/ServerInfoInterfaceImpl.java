package com.pinganfu.hippoconsoleweb.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.annotation.Resource;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import com.pinganfu.hippoconsoleweb.dal.ServerInfoDao;
import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.model.ServerInfoDo;
import com.pinganfu.hippoconsoleweb.service.GetJmxInfoInterface;
import com.pinganfu.hippoconsoleweb.service.ServerInfoInterface;
import com.pinganfu.hippoconsoleweb.service.ZkControlUtil;
import com.pinganfu.hippoconsoleweb.util.ZkUtils;

public class ServerInfoInterfaceImpl implements ServerInfoInterface  {
	
	private Logger logger = LoggerFactory.getLogger(ServerInfoInterfaceImpl.class);
	@Resource
	private ServerInfoDao serverInfoDao;
	
	@Override
	public List<ServerInfoBean> loadServerInfoList(ServerInfoBean model)throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		List<ServerInfoBean> listF = new ArrayList<ServerInfoBean>();
		try{
			List<ServerInfoDo> list = serverInfoDao.loadServerInfoList(info);
			if(list !=null && list.size()>0){
				for(ServerInfoDo infoDo : list){
					ServerInfoBean bean = new ServerInfoBean();
					BeanUtils.copyProperties(infoDo, bean);
					
					listF.add(bean);
				}
			}
		}catch(Exception ex){
			logger.error("load server info list error "+ ex.toString());
		}
		
		return listF;
	}
	
	
	@Override
	public int insertServerInfo(ServerInfoBean model) throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		return serverInfoDao.insertServerInfo(info);
	}
	
	@Override
	public ServerInfoBean findServerInfo(ServerInfoBean model) throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		ServerInfoDo infoDo = serverInfoDao.findServerInfo(info);
		ServerInfoBean target = new ServerInfoBean();
		BeanUtils.copyProperties(infoDo, target);
		return target;
	}
	
	@Override
	public int findServerInfoCount(ServerInfoBean model) throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		return serverInfoDao.findServerInfoCount(info);
	}
	
	@Override
	public int delServerInfo(ServerInfoBean model) throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		return serverInfoDao.delServerInfo(info);
	}
	
	@Override
	public int editServerInfo(ServerInfoBean model) throws Exception {
		ServerInfoDo info = new ServerInfoDo();
		BeanUtils.copyProperties(model, info);
		int row = serverInfoDao.editServerInfo(info);
		return row;
	}
	
	@Override
	public ServerInfoBean getJmxValue(ServerInfoBean info)throws Exception {
		logger.info("  load jmx start  ");
		ServerInfoBean sib = getJmxValue0(info);
		logger.info("  load jmx end  ");
		return sib;
	}
	
	/**
     * 返回jmx中的数据
     * @param info
     * @return
     * @throws Exception
     */
    private ServerInfoBean getJmxValue0(ServerInfoBean info)throws Exception{
        
        String[] ipAddress = info.getServer_id().split(":");
        
        logger.info("--ipAddress.length:"+ipAddress.length);
        if(ipAddress != null && ipAddress.length>0){
            String ip = ipAddress[0];
            String port = info.getJmxPort();
            logger.info(" ip : "+ ip);
            logger.info(" port : "+port);
            if(port !=null && !port.equals("")){
            	GetJmxInfoInterface getJmxInfoInterface = new GetJmxInfoInterfaceImpl();
            	logger.info(" BrokerName "+info.getBrokerName());
                ServerInfoBean serverBean = getJmxInfoInterface.getBrokerView(ip, port,info.getBrokerName());
                info.setBrokerName(serverBean.getBrokerName());
                info.setBrokerVersion(serverBean.getBrokerVersion());
                info.setStore(serverBean.getStore());
                info.setMemory(serverBean.getMemory());
                info.setMemoryLimit(serverBean.getMemoryLimit());
                info.setMemoryPercentUsage(serverBean.getMemoryPercentUsage());
                info.setStoreLimit(serverBean.getStoreLimit());
                info.setStorePercentUsage(serverBean.getStorePercentUsage());
                info.setDataDirectory(serverBean.getDataDirectory());
                info.setStarted(serverBean.getStarted());
                info.setCurrentUsedCapacity(serverBean.getCurrentUsedCapacity());
                info.setEngineData(serverBean.getEngineData());
                info.setEngineName(serverBean.getEngineName());
                info.setEngineSize(serverBean.getEngineSize());
                info.setConn(serverBean.getConn());
                info.setClient(serverBean.getClient());
            }
        }else{
            info.setBrokerName("");
            info.setBrokerVersion("");
            info.setStore("0");
            info.setMemory("0");
            info.setMemoryLimit("0");
            info.setMemoryPercentUsage("0");
            info.setStoreLimit("0");
            info.setStorePercentUsage("0");
            info.setDataDirectory("");
            info.setStarted("false");
        }
        return info;
    }
	
	@Override
	public ServerInfoBean loadBucketCount(String clusterName,ServerInfoBean info, String zkAddress) throws Exception{
		ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
		return (ServerInfoBean) ZkControlUtil.loadBucketCount(clusterName, info, zkClient);
	}
	
	@Override
	public List<ServerInfoBean> loadServerInfoByZk(String clusterName, String zkAddress) throws Exception{
		ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
		return ZkControlUtil.loadServerInfoBeanByZk(clusterName, zkClient);
	}
	
	@Override
	public Map<Integer, Vector<String>> loadServerTable(String zkAddress,String clusterName) throws Exception{
		ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
		return ZkControlUtil.loadServerTable(zkClient, clusterName,"mtable");
	}
	
	
	
}
