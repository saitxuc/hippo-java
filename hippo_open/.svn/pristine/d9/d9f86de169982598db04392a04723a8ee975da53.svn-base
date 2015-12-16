package com.pinganfu.hippoconsoleweb.rule.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.pinganfu.hippoconsoleweb.model.ServerModel;
import com.pinganfu.hippoconsoleweb.model.TongModel;

public class ServerArithmetic {	
	
	/**
	 * 特定的备机变主机
	 * @param list
	 * @param server
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> toMaster(List<ServerModel> list,ServerModel server)throws Exception{
		List<ServerModel> newServerList = new LinkedList<ServerModel>();
		List<TongModel> slaveList = new ArrayList<TongModel>();
		for(ServerModel newServer : list){
			if(server.getServerName() !=null && !server.getServerName().equals(newServer.getServerName()) ){
				//--备机升主机
				List<TongModel> downTongList = server.getTongList();//--获取宕机的桶信息
				if(downTongList !=null && downTongList.size()>0){
					for(TongModel downTong:downTongList){
						if(downTong.getIsMaster()==1){ //--服务器中的主桶信息
							if(newServer.getTongList() !=null && newServer.getTongList().size()>0){
								for(TongModel tong : newServer.getTongList()){
									if(tong.getLevel()==1 && tong.getTongMark().equals(downTong.getTongMark())){
										TongModel newTong = new TongModel();
										newTong.setIsMaster(tong.getIsMaster());
										newTong.setLevel(tong.getLevel());
										newTong.setTongMark(tong.getTongMark());
										slaveList.add(newTong);
										tong.setIsMaster(1);
										tong.setLevel(0);
									}
								}
							}
						}
					}
				}
				newServerList.add(newServer);
			}
		}
		//--排序
		Collections.sort(newServerList, new SortByTongNumberDesc());
		if(slaveList !=null && slaveList.size()>0){
			for(TongModel tong : slaveList){
				
				for(ServerModel newServer : newServerList){
					int slave = 0;
					List<TongModel> tongL = newServer.getTongList();
					if(tongL !=null && tongL.size()>0){
						for(TongModel tStr : tongL){
							if(tong.getTongMark().equals(tStr.getTongMark())){
								slave++;
							}
						}
					}
					if(slave == 0){
						tongL.add(tong);
					}
				}
			}
		}
		return newServerList;
	}
	
	/**
	 * 分配备机
	 * @param list
	 * @param server
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> setBackUp(List<ServerModel> list,ServerModel server)throws Exception{
		List<TongModel> tongList = server.getTongList();
		List<TongModel> tongLevel1 = new LinkedList<TongModel>();
		List<TongModel> tongLevel2 = new LinkedList<TongModel>();
		for(TongModel tong : tongList){
			if(tong.getLevel() == 1 ){
				tongLevel1.add(tong);
			}
			if(tong.getLevel() == 2){
				tongLevel2.add(tong);
			}
		}
		list = this.setTong(tongLevel1, list);
		list = this.setTong(tongLevel2, list);
		return list;
	}
	
	/**
	 * 设定桶信息
	 * @param tonglist
	 * @param serverList
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> setTong(List<TongModel> tonglist,List<ServerModel> serverList)throws Exception{
		
		for(TongModel tl1 : tonglist){ //--备1 
			List<ServerModel> serList = new LinkedList<ServerModel>();
			for(ServerModel ser : serverList){
				if(ser.getTongList() !=null && ser.getTongList().size()>0){
					int s1 = 0;
					for(TongModel t1 : ser.getTongList()){
						if(t1.getTongMark().equals(tl1.getTongMark())){
							s1++ ;
						}
					}
					if(s1 == 0){//--找出没有相应桶的服务器
						serList.add(ser);
					}
				}
			}
			
			Collections.sort(serList, new SortByTongNumber());
			if(serList !=null && serList.size()>0){
				ServerModel ser0 = (ServerModel)serList.get(0);
				for(ServerModel ser : serverList){
					if(ser.getServerName().equals(ser0.getServerName())){
						ser.getTongList().add(tl1);
					}
				}
			}
		}
		return serverList;
	}
	
	/**
	 * 根据每个server中的主桶进行排序
	 * @param serverList
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> getServerDesc(List<ServerModel> serverList)throws Exception{
		if(serverList !=null && serverList.size()>0){
			for(ServerModel server : serverList){
				if(server.getTongList() !=null && server.getTongList().size()>0){
					Collections.sort(server.getTongList(), new SortByMaster());
				}
			}
		}
		Collections.sort(serverList, new SortByMasterTongNumber());
		return serverList;
	}
	
	/**
	 * 获取最大公约数
	 * @param tongSum
	 * @param serverSum
	 * @return
	 * @throws Exception
	 */
	public int getDivisor(int tongSum,int serverSum)throws Exception{
		int divisor = 0;
		divisor = tongSum / serverSum;
		return divisor;
	}
	
	/**
	 * 获取总桶数
	 * @param serverList
	 * @return
	 * @throws Exception
	 */
	public int getTongSum(List<ServerModel> serverList)throws Exception{
		int tongSum = 0;
		if(serverList != null && serverList.size()>0){
			for(ServerModel server:serverList){
				if(server.getTongList() !=null && server.getTongList().size()>0){
					for(TongModel tong:server.getTongList()){
						if(tong.getIsMaster() == 1){
							tongSum = tongSum + 1;
						}
					}
				}
			}
		}
		return tongSum;
	}
	
	/**
	 * 获取桶信息
	 * @param serverList
	 * @return
	 * @throws Exception
	 */
	public List<TongModel> getTongList(List<ServerModel> serverList)throws Exception{
		List<TongModel> list = new ArrayList<TongModel>();
		if(serverList != null && serverList.size()>0){
			for(ServerModel server:serverList){
				if(server.getTongList() !=null && server.getTongList().size()>0){
					for(TongModel tong:server.getTongList()){
						if(tong.getIsMaster() == 1){
							TongModel t = new TongModel();
							t.setTongMark(tong.getTongMark());
							t.setLevel(tong.getLevel());
							t.setIsMaster(tong.getIsMaster());
							list.add(t);
						}
					}
				}
			}
		}
		return list;
	}
	
	/**
	 * 在新的服务器中添加桶
	 * @param serverList
	 * @param divisor
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> addMasterTongIntoServer(List<ServerModel> serverList,int divisor,ServerModel addServer)throws Exception{

		int isBreak = 0;
		if(serverList !=null && serverList.size()>0){
			for(int TongNumber=0;TongNumber < divisor; ){
				ServerModel server = (ServerModel)serverList.get(0);
				List<TongModel> tongList = server.getTongList();
				if(tongList !=null && tongList.size()>0){
					int tMasterNum = 0;
					for(TongModel tong:tongList){
						if(tong.getIsMaster() == 1){
							tMasterNum ++ ;
						}
					}
					if(tMasterNum > 1){
						List<TongModel> addServerTong = addServer.getTongList();
						if(addServerTong !=null && addServerTong.size()>0){
							addServerTong.add(tongList.get(0));
						}else{
							addServerTong = new LinkedList<TongModel>();
							addServerTong.add(tongList.get(0));
						}
						addServer.setTongList(addServerTong);
						tongList.remove(0);
						TongNumber++;
					}else{
						isBreak++;
					}
				}
				Collections.sort(serverList, new SortByMasterTongNumber());
				if(isBreak == serverList.size()){
					break;
				}
			}
			serverList.add(addServer);
		}
		return serverList;
	}
	
	/**
	 * 在新服务器中添加备桶
	 * @param serverList
	 * @param divisor
	 * @param addServer
	 * @return
	 * @throws Exception
	 */
	public List<ServerModel> addFirstSlaveTong(List<ServerModel> serverList,int divisor,ServerModel addServer)throws Exception{
		int isBreak = 0;
		if(serverList !=null && serverList.size()>0){
			for(ServerModel server:serverList){
				if(server.getServerName().equals(addServer.getServerName())){
					addServer.setServerName(server.getServerName());
					addServer.setTongList(server.getTongList());
					addServer.setTongType(server.getTongType());
				}
			}
			Collections.sort(serverList, new SortByTongNumberDesc());
			for(int TongNumber=0;TongNumber < divisor; ){
				ServerModel server = (ServerModel)serverList.get(0);
				if(server.getServerName().equals(addServer.getServerName())){
					server = (ServerModel)serverList.get(1);
				}
				List<TongModel> tongList = server.getTongList();
				if(tongList !=null && tongList.size()>0){
					List<TongModel> addServerTong = addServer.getTongList();
					if(addServerTong == null || addServerTong.size()== 0){
						addServerTong = new LinkedList<TongModel>();
					}
					for(TongModel t : tongList){
						if(t.getLevel() == 1){
							addServerTong.add(t);
							tongList.remove(t);
							addServer.setTongList(addServerTong);
							break;
						}
					}
					TongNumber++;
				}else{
					isBreak++;
				}
				Collections.sort(serverList, new SortByTongNumberDesc());
				if(isBreak == serverList.size()){
					break;
				}
			}
		}
		return serverList;
	}
	
	public List<ServerModel> addSecondSlaveTong(List<ServerModel> serverList,int divisor,ServerModel addServer)throws Exception{
		int isBreak = 0;
		if(serverList !=null && serverList.size()>0){
			//--获取服务器中的数据并将相关的服务器的内容赋值给addserver
			for(ServerModel server:serverList){
				if(server.getServerName().equals(addServer.getServerName())){
					addServer.setServerName(server.getServerName());
					addServer.setTongList(server.getTongList());
					addServer.setTongType(server.getTongType());
				}
			}
			Collections.sort(serverList, new SortByTongNumberDesc());
			int next = 0;
			String serName = "";
			for(int TongNumber=0;TongNumber < divisor; ){
				
				ServerModel server = (ServerModel)serverList.get(next);
				if(server.getServerName().equals(addServer.getServerName())){
					next++;
					server = (ServerModel)serverList.get(next);
				}
				List<TongModel> tongList = server.getTongList();
				if(tongList !=null && tongList.size()>0){
					List<TongModel> addServerTong = addServer.getTongList();
					if(addServerTong == null || addServerTong.size()== 0){
						addServerTong = new LinkedList<TongModel>();
					}
					TongModel newTong = new TongModel();
					for(TongModel t : tongList){
						if(t.getLevel() == 2){
							newTong.setIsMaster(t.getIsMaster());
							newTong.setLevel(t.getLevel());
							newTong.setTongMark(t.getTongMark());
							break;
						}
					}
					
					int isSame = 0;
					if(addServer.getTongList() !=null && addServer.getTongList().size()>0){
						for(TongModel t:addServer.getTongList()){
							if(t.getTongMark().equals(newTong.getTongMark())){
								isSame++;
							}
						}
					}
					if(isSame == 0){
						addServerTong.add(newTong);
						for(TongModel tt:tongList){
							if(tt.getTongMark().equals(newTong.getTongMark())){
								tongList.remove(tt);
								break;
							}
						}
						addServer.setTongList(addServerTong);
						TongNumber++;
					}else{
						serName = server.getServerName();
					}
				}else{
					isBreak++;
				}
				Collections.sort(serverList, new SortByTongNumberDesc());
				ServerModel oldServer = (ServerModel)serverList.get(next);
				if(oldServer !=null ){
					if(oldServer.getServerName().equals(serName)){
						next++;
					}
				}
				if(next == serverList.size()){
					break;
				}
				if(isBreak == serverList.size()){
					break;
				}
			}
		}
		return serverList;
	}
	
	public List<ServerModel> addMasterNew(List<ServerModel> serverList,int divisor,ServerModel addServer,List<TongModel> oldTongList)throws Exception{
		if(oldTongList !=null && oldTongList.size()>0){
			List<TongModel> t2List = new ArrayList<TongModel>();
			int isBreak = 0;
			for(TongModel oldT:oldTongList){
				int isNull = 0;
				if(serverList !=null && serverList.size() > 0){
					for(ServerModel ser:serverList){
						if(ser.getTongList() !=null && ser.getTongList().size()>0){
							for(TongModel newT : ser.getTongList()){
								if(newT.getTongMark().equals(oldT.getTongMark())){
									isNull ++;
								}
							}
						}
					}
				}
				if(isNull == 0 ){
					TongModel t2 = new TongModel();
					t2.setIsMaster(1);
					t2.setLevel(0);
					t2.setTongMark(oldT.getTongMark());
					t2List.add(t2);
					isBreak++;
				}
				if(isBreak == divisor){
					break;
				}
			}
			addServer.setTongList(t2List);
			serverList.add(addServer);
			//--判断是否还有多余的
			for(TongModel oldT:oldTongList){
				int isNull = 0;
				if(serverList !=null && serverList.size() > 0){
					for(ServerModel ser:serverList){
						if(ser.getTongList() !=null && ser.getTongList().size()>0){
							for(TongModel newT : ser.getTongList()){
								if(newT.getTongMark().equals(oldT.getTongMark())){
									isNull ++;
								}
							}
						}
					}
				}
				if(isNull == 0 ){
					Collections.sort(serverList, new SortByMasterTongNumber());
					for(ServerModel sm:serverList){
						if(sm.getTongList() !=null && sm.getTongList().size()>0){
							int ot3 = 0;
							for(TongModel t3 : sm.getTongList()){
								if(oldT.getTongMark().equals(t3.getTongMark())){
									ot3++;
								}
							}
							if(ot3==0){
								TongModel t4 = new TongModel();
								t4.setIsMaster(1);
								t4.setLevel(0);
								t4.setTongMark(oldT.getTongMark());
								sm.getTongList().add(t4);
							}
						}
					}
				}
				//--
			}
		}
		return serverList;
	}
	
	
}
