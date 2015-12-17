package com.hippoconsoleweb.rule.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.hippoconsoleweb.model.ServerModel;
import com.hippoconsoleweb.model.TongModel;
import com.hippoconsoleweb.rule.ChooseServerRuleInterface;
import com.hippoconsoleweb.rule.common.ServerArithmetic;
import com.hippoconsoleweb.rule.common.SortByTongName;

public class ChooseServerRuleImpl implements ChooseServerRuleInterface {
	private Logger logger=LoggerFactory.getLogger(ChooseServerRuleImpl.class);
	@Resource
	private ServerArithmetic serverArithmetic; 

	@Override
	public List<ServerModel> addServer(List<ServerModel> list , ServerModel addServer) throws Exception {
		list = serverArithmetic.getServerDesc(list);
		int tongSum = serverArithmetic.getTongSum(list);
		int divisor = serverArithmetic.getDivisor(tongSum, list.size()+1);
		list = serverArithmetic.addMasterTongIntoServer(list, divisor, addServer);
		list = serverArithmetic.addFirstSlaveTong(list, divisor, addServer);
		list = serverArithmetic.addSecondSlaveTong(list, divisor, addServer);
		return list;
	}
	
	@Override
	public List<ServerModel> delServer(List<ServerModel> list , ServerModel downServer) throws Exception {
		List<ServerModel> newServerList = serverArithmetic.toMaster(list, downServer);
		newServerList = serverArithmetic.setBackUp(newServerList, downServer);
		return newServerList;
	}
	
	@Override
	public List<ServerModel> analyseConfiguration(String configuration)throws Exception {
		List<ServerModel> list = new LinkedList<ServerModel>();
		JSONArray jsonArry = JSON.parseArray(configuration);
		if(jsonArry !=null && jsonArry.size() >0){
			for(int i=0;i<jsonArry.size();i++){
				ServerModel server = JSON.parseObject(jsonArry.getString(i), ServerModel.class);
				JSONArray tongArray = JSON.parseArray(server.getTongType().toString());
				if(tongArray !=null && tongArray.size()>0){
					List<TongModel> tongList = new LinkedList<TongModel>();
					for(int j=0;j<tongArray.size();j++){
						TongModel tong = JSON.parseObject(tongArray.getString(j),TongModel.class);
						tongList.add(tong);
					}
					server.setTongList(tongList);
				}else{
					logger.equals("tongArray value is null");
				}
				list.add(server);
			}
		}else{
			logger.error("serverArry value is null!");
		}
		return list;
	}
	
	@Override
	public List<TongModel> loadTongList(List<ServerModel> serverList)throws Exception {
		List<TongModel> list = new ArrayList<TongModel>();
		if(serverList !=null && serverList.size()>0){
			for(ServerModel server : serverList){
				List<TongModel> tList = server.getTongList();
				if(tList != null && tList.size()>0){
					for(TongModel t : tList){
						if(t.getIsMaster() == 1){
							list.add(t);
						}
					}
				}
			}
		}else{
			logger.error("serverList is null or is zero!");
		}
		Collections.sort(list, new SortByTongName());
		return list;
	}
	
	@Override
	public ServerModel loadServerByName(List<ServerModel> serverList,String serverName)throws Exception {
		ServerModel sm = new ServerModel();
		if(serverList !=null && serverList.size()>0){
			for(ServerModel ser:serverList){
				if(ser.getServerName().equals(serverName)){
					sm.setServerName(ser.getServerName());
					sm.setTongList(ser.getTongList());
					sm.setTongType(ser.getTongType());
				}
			}
		}
		return sm;
	}
	
	@Override
	public List<ServerModel> chooseServer(List<ServerModel> list,String serverNameStr) throws Exception {
		list = serverArithmetic.getServerDesc(list);
		int tongSum = serverArithmetic.getTongSum(list);
		int divisor = serverArithmetic.getDivisor(tongSum, list.size()+1);
		int delNum = 0;
		int serSum = list.size();
		List<TongModel> sumTongList = serverArithmetic.getTongList(list);//--获取原始的桶列表
		
		if(list!=null && list.size()>0 ){
			for(ServerModel ser:list){
				String serverNameStr1 = ","+serverNameStr+",";
				int count = serverNameStr1.indexOf(","+ser.getServerName()+",");
				if(count == -1){
					list = this.delServer(list,ser);
					delNum++;
				}
			}
			
			System.out.println("========开始添加");
			
			String[] strName = serverNameStr.split(",");
			if(strName !=null && strName.length>0){
				for(int i=0;i<strName.length;i++){
					int add = 0;
					for(ServerModel ser:list){
						if(ser.getServerName().equals(strName[i])){
							add++;
						}
					}
					if(add == 0){
						ServerModel sm = new ServerModel();
						sm.setServerName(strName[i]);
						if(serSum - delNum >=3){
							list = serverArithmetic.addMasterTongIntoServer(list, divisor, sm);
							list = serverArithmetic.addFirstSlaveTong(list, divisor, sm);
							list = serverArithmetic.addSecondSlaveTong(list, divisor, sm);
						}else{
							list = serverArithmetic.addMasterNew(list, divisor, sm, sumTongList);
						}
					}
				}
			}
			System.out.println("========结束添加");
		}
		
		return list;
	}
	
}
