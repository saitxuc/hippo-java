package com.pinganfu.hippoconsoleweb.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.pinganfu.hippoconsoleweb.common.LoadPropertiesData;
import com.pinganfu.hippoconsoleweb.model.ZkManageTree;
import com.pinganfu.hippoconsoleweb.service.ZkManageInterface;

/**
 * 报警设定功能
 * @author gusj
 * @version $Id: AlarmManageController.java, v 0.1 2014年12月29日 下午1:46:24 gusj Exp $
 */
@Controller
@RequestMapping("server")
public class ZkManageController {

	private Logger logger=LoggerFactory.getLogger(ZkManageController.class);
	
	@Resource
	private LoadPropertiesData loadPropertiesData;
	@Resource
	private ZkManageInterface zkManageInterface;
	
	/**
	 * 进入报警设定页面
	 * @param request
	 * @param model
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/zk_manage", method = RequestMethod.GET)
    public String alarm_list(ModelMap model) throws Exception{
        return "/server/server_list";
    }
	
	@RequestMapping(value = "/zkPage", method = RequestMethod.GET)
    public String zkPage(ModelMap model) throws Exception{
        return "/zookeeper/zk_list";
    }
	
	/**
	 * 获取zk树列表
	 * @param request
	 * @param model
	 * @param zkDo
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/zk_tree_list", method = RequestMethod.GET)
    public @ResponseBody 
    List<Map<String,Object>> loadTreeList(ModelMap model,ZkManageTree zkDo) throws Exception{
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try{
			resultList = zkManageInterface.loadZkList(zkDo);
		}catch(Exception ex){
			logger.error(""+ex.toString());
		}
        return resultList;
    }
	
	
	@RequestMapping(value = "/zk_first_tree_list", method = RequestMethod.GET)
    public @ResponseBody 
    List<Map<String,Object>> zkFirstTreeList(ModelMap model,ZkManageTree zkDo) throws Exception{
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try{
			resultList = zkManageInterface.loadZkRootList(zkDo);
		}catch(Exception ex){
			logger.error(""+ex.toString());
		}
        return resultList;
    }
	
	
	
	@RequestMapping(value="/zk_read",method = RequestMethod.POST)
	public @ResponseBody Map<String,String> zkRead(ModelMap model,ZkManageTree zkDo)throws Exception{
		Map<String,String> map = new HashMap<String, String>();
		String resultStr = "";
		try{
			resultStr = zkManageInterface.loadZkRead(zkDo);
		}catch(Exception ex){
			logger.error(ex.toString());
		}
		map.put("str", resultStr);
		return map ;
	}

	
	@RequestMapping(value="/zk_write",method = RequestMethod.POST)
	public @ResponseBody Map<String,String> zkWrite(ModelMap model,ZkManageTree zkDo)throws Exception{
		Map<String,String> map = new HashMap<String, String>();
		boolean flag = false;
		String i = "0";
		try{
			flag = zkManageInterface.ZkWrite(zkDo);
			if(flag == true){
				i = "1";
			}
		}catch(Exception ex){
			logger.error(ex.toString());
		}
		map.put("success", i);
		return map ;
	}
	
	@RequestMapping(value="/zk_create",method = RequestMethod.POST)
	public @ResponseBody Map<String,String> zkCreate(ModelMap model,ZkManageTree zkDo)throws Exception{
		Map<String,String> map = new HashMap<String, String>();
		boolean flag = false;
		String i = "0";
		try{
			flag = zkManageInterface.ZkCreate(zkDo);
			if(flag == true){
				i = "1";
			}
		}catch(Exception ex){
			logger.error(ex.toString());
		}
		map.put("success", i);
		return map ;
	}
	
	@RequestMapping(value="/zk_Delete",method = RequestMethod.POST)
	public @ResponseBody Map<String,String> zkDelete(ModelMap model,ZkManageTree zkDo)throws Exception{
		Map<String,String> map = new HashMap<String, String>();
		boolean flag = false;
		String i = "0";
		try{
			flag = zkManageInterface.ZkDelete(zkDo);
			if(flag == true){
				i = "1";
			}
		}catch(Exception ex){
			logger.error(ex.toString());
		}
		map.put("success", i);
		return map ;
	}
	
	@RequestMapping(value="/showServerData",method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> showServerData(ModelMap model)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		Map<Integer,Vector<String>> map1 = new HashMap<Integer, Vector<String>>();
//		map1 = zkManageInterface.loadTable();
		List<Vector<String>> list = new ArrayList<Vector<String>>();
		if(map1 != null){
			for(int i=0;i<map1.size();i++){
				Vector<String> vt = (Vector<String>)map1.get(i);
				list.add(vt);
			}
		}
		map.put("table", list);
		return map;
	}
	
	@RequestMapping(value="/dataReset",method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> dataReset(ModelMap model,String clusterName)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		boolean flag = true;
		flag = zkManageInterface.zkDataRest(clusterName);
		if(flag){
			map.put("success", "1");
		}else{
			map.put("success", "2");
		}
		return map;
	}
	
	
	
	
}
