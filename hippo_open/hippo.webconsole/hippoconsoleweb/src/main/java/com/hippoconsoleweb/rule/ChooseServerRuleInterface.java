package com.hippoconsoleweb.rule;

import java.util.List;

import com.hippoconsoleweb.model.ServerModel;
import com.hippoconsoleweb.model.TongModel;

public interface ChooseServerRuleInterface {

	/**
	 * 添加服务器
	 * @param list
	 * @param sm
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> addServer(List<ServerModel> list ,ServerModel sm)throws Exception;
	
	/**
	 * 删除服务器
	 * @param list
	 * @param sm
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> delServer(List<ServerModel> list , ServerModel sm)throws Exception;
	 
	/**
	 * 分析zk的json参数数据
	 * @param configuration
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> analyseConfiguration(String configuration)throws Exception;
	
	/**
	 * 获取Tong列表
	 * @param serverList
	 * @return
	 * @throws Exception
	 */
	public List<TongModel> loadTongList(List<ServerModel> serverList)throws Exception;
	
	public ServerModel loadServerByName(List<ServerModel> serverList,String serverName)throws Exception;
	
	public List<ServerModel> chooseServer(List<ServerModel> list,String serverNameStr)throws Exception;
}
