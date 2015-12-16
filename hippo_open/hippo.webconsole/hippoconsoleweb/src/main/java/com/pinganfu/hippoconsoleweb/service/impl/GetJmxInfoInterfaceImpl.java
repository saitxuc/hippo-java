package com.pinganfu.hippoconsoleweb.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippoconsoleweb.model.ClientConnectors;
import com.pinganfu.hippoconsoleweb.model.ClientModel;
import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.service.GetJmxInfoInterface;
import com.pinganfu.hippoconsoleweb.util.JMXHelper;

public class GetJmxInfoInterfaceImpl implements GetJmxInfoInterface {

	private static String HIPPO_BROKER_NAME = "org.apache.hippo:type=Broker,brokerName=";
	private static Logger logger = LoggerFactory.getLogger(GetJmxInfoInterfaceImpl.class);
	
	@Override
	public ServerInfoBean getBrokerView(String ip,String port,String bkName)throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append(HIPPO_BROKER_NAME);
		sb.append(bkName.trim());
		String ObjectName = sb.toString();
		ServerInfoBean info = new ServerInfoBean();
		JMXConnector jmxConnector = null;
		try{
			jmxConnector = JMXHelper.getConnection(ip, Integer.parseInt(port));
			logger.error("load Object Name:"+ObjectName);
	        ObjectName mbeanName = new ObjectName(ObjectName);
	        String brokerName = JMXHelper.getAttribute(jmxConnector, mbeanName, "BrokerName").toString();
	        String brokerVersion = JMXHelper.getAttribute(jmxConnector, mbeanName, "BrokerVersion").toString();
	        info.setBrokerName(brokerName);
	        info.setBrokerVersion(brokerVersion);
	        info.setStore(JMXHelper.getAttribute(jmxConnector, mbeanName, "StorePercentUsage").toString());
	        info.setMemory(JMXHelper.getAttribute(jmxConnector, mbeanName, "MemoryPercentUsage").toString());
	        info.setMemoryLimit(JMXHelper.getAttribute(jmxConnector, mbeanName, "MemoryLimit").toString());
	        info.setMemoryPercentUsage(JMXHelper.getAttribute(jmxConnector, mbeanName, "MemoryPercentUsage").toString());
	        info.setStoreLimit(JMXHelper.getAttribute(jmxConnector, mbeanName, "StoreLimit").toString());
	        info.setStorePercentUsage(JMXHelper.getAttribute(jmxConnector, mbeanName, "StorePercentUsage").toString());
	        info.setDataDirectory(JMXHelper.getAttribute(jmxConnector, mbeanName, "DataDirectory").toString());
	        info.setStarted(JMXHelper.getAttribute(jmxConnector, mbeanName, "Started").toString());
	        String objectName = JMXHelper.getAttribute(jmxConnector, mbeanName, "StoreEngineAdapterObjectName").toString();
	        List<String> connectorName = (List<String>)JMXHelper.getAttribute(jmxConnector, mbeanName, "ConnectorName");
	        info = getEngineView(objectName, info,jmxConnector);
	        info = getConnectorView(connectorName,info,jmxConnector);
	        if(info.getConn() != null){
	        	String connector = info.getConn().get(0).getObjectName();
	        	List<ClientModel> client = this.getJmxClientList(ObjectName, jmxConnector,connector);
	            if(client != null && client.size()>0){
	            	info.setClient(client);
	            }
	        }
		}catch(Exception ex){
			logger.error("load jmx error:"+ex.toString());
			ex.printStackTrace();
			throw new Exception(ex);
		}finally {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		return info;
	}
	
	@Override
	public int getConnectionCount(String ip,String port,String objectName) throws Exception {
		JMXServiceURL address = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi");
		ObjectName mbeanName = new ObjectName(objectName);
		String count = JMXHelper.executeMethod(address, mbeanName, "connectionCount", null, null).toString();
		return  Integer.parseInt(count);
	}
	
	@Override
	public int getSessionCount(String ip, String port, String objectName)throws Exception {
		JMXServiceURL address = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi");
		ObjectName mbeanName = new ObjectName(objectName);
		String count = JMXHelper.executeMethod(address, mbeanName, "sessionCount", null, null).toString();
		return  Integer.parseInt(count);
	}
	
	@Override
	public List<ClientModel> getJmxConnection(String ip, String port,String connectorName,String bkName) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append(HIPPO_BROKER_NAME);
		sb.append(bkName.trim());
		String objectName = sb.toString();
		JMXConnector jmxConnector = null;
		List<ClientModel> client = new ArrayList<ClientModel>();
		try{
			jmxConnector = JMXHelper.getConnection(ip, Integer.parseInt(port));
			client = this.getJmxClientList(objectName,jmxConnector,connectorName);
		}catch(Exception ex){
			logger.equals("load jmx error:"+ex.toString());
		}finally{
			if(jmxConnector != null){
				jmxConnector.close();
			}
		}
		
		return client ;
	}
	
	private ServerInfoBean getEngineView(String ObjectName,ServerInfoBean info ,JMXConnector jmxConnector)throws Exception{
		ObjectName mbeanName = new ObjectName(ObjectName);
		info.setEngineSize(JMXHelper.getAttribute(jmxConnector, mbeanName, "Size").toString());
		info.setEngineName(JMXHelper.getAttribute(jmxConnector, mbeanName, "Name").toString());
		info.setEngineData(JMXHelper.getAttribute(jmxConnector, mbeanName, "Data").toString());
		info.setCurrentUsedCapacity(JMXHelper.getAttribute(jmxConnector, mbeanName, "CurrentUsedCapacity").toString());
		return info;
	}
	
	private ServerInfoBean getConnectorView(List<String> ObjectName,ServerInfoBean info ,JMXConnector jmxConnector)throws Exception{
		List<ClientConnectors> list = new ArrayList<ClientConnectors>();
		if(ObjectName !=null && ObjectName.size()>0){
			for(String oName :ObjectName ){
				ObjectName mbeanName = new ObjectName(oName);
				ClientConnectors client = new ClientConnectors();
				client.setConnEnabled(JMXHelper.getAttribute(jmxConnector, mbeanName, "StatisticsEnabled").toString());
				client.setConnStarted(JMXHelper.getAttribute(jmxConnector, mbeanName, "Started").toString());
				Object obj = JMXHelper.getAttribute(jmxConnector, mbeanName, "StartException");
				client.setObjectName(oName);
				if(obj !=null ){
					client.setStartException(obj.toString());
				}
				list.add(client);
			}
			info.setConn(list);
		}
		
		return info;
	}
	
	
	
	private List<ClientModel> getJmxClientList(String ObjectName,JMXConnector jmxConnector,String connectorName)throws Exception{
		ObjectName mbeanName = new ObjectName(ObjectName);
		List<Map<String,String>> clientObjectNames = (List<Map<String,String>>)JMXHelper.getAttribute(jmxConnector, mbeanName, "ClientObjectNames");
		List<ClientModel> list = new ArrayList<ClientModel>();
        if(clientObjectNames !=null && clientObjectNames.size()>0){
        	for(Map<String,String> objectName : clientObjectNames){
        		ClientModel client = new ClientModel();
        		String oName = objectName.get(connectorName);
        		if(oName != null){
        			ObjectName mbeanClientName = new ObjectName(oName);
            		String clientId = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "ClientId").toString();
            		client.setClientId(clientId);
            		String remoteAddress = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "RemoteAddress").toString();
            		client.setRemoteAddress(remoteAddress);
            		String active = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "Active").toString();
            		client.setActive(active);
            		String blocked = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "Blocked").toString();
            		client.setBlocked(blocked);
            		String connected = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "Connected").toString();
            		client.setConnected(connected);
            		String slow = JMXHelper.getAttribute(jmxConnector, mbeanClientName, "Slow").toString();
            		client.setSlow(slow);
            		client.setObjectName(oName);
            		list.add(client);
        		}
        	}
        }
		return list;
	}

}
