package com.hippoconsoleweb.service;

import java.util.List;

import com.hippoconsoleweb.model.ClientModel;
import com.hippoconsoleweb.model.ServerInfoBean;

public interface GetJmxInfoInterface {

	public ServerInfoBean getBrokerView(String ip,String port,String bkName)throws Exception;
	
	public int getConnectionCount(String ip,String port,String objectName)throws Exception;
	
	public int getSessionCount(String ip,String port,String objectName)throws Exception;
	
	public List<ClientModel> getJmxConnection(String ip,String port,String objectName,String bkName)throws Exception;
	
}
 