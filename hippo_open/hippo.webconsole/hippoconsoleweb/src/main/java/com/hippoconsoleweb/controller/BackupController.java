package com.hippoconsoleweb.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hippoconsoleweb.common.LoadPropertiesData;
import com.hippoconsoleweb.model.ZkClusterBackUpInfoBean;
import com.hippoconsoleweb.model.ZkDataServersInfoBean;
import com.hippoconsoleweb.model.ZkTablesInfoBean;
import com.hippoconsoleweb.service.ZkBackupInfoInterface;

@Controller
@RequestMapping("backup")
public class BackupController {

	@Resource
	private LoadPropertiesData loadPropertiesData;
	@Resource
	private ZkBackupInfoInterface zkBackupInfoInterface;
	
	@RequestMapping(value = {"/backupList"}, method = RequestMethod.GET)
	public String index(ModelMap model,String name) throws Exception{
        return "/backup/backup_list";
    }
	
	@RequestMapping(value={"/loadBackupList"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadBackupList(ModelMap model,ZkClusterBackUpInfoBean info,int size ,int page)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		int start = (page-1)*size;
		List<ZkClusterBackUpInfoBean> list = zkBackupInfoInterface.loadBackUpList(start, size, info);
		int total = zkBackupInfoInterface.loadBackUpCount(info);
		map.put("total", total);
		map.put("rows", list);
		return map;
	}
	
	@RequestMapping(value={"/showTables"},method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> showTables(ModelMap model,ZkClusterBackUpInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ZkTablesInfoBean> list = zkBackupInfoInterface.loadTablesInfoByClusterId(info.getId());
		map.put("list", list);
		return map;
	}
	
	@RequestMapping(value={"/showDataServers"},method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> showDataServers(ModelMap model,ZkClusterBackUpInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		List<ZkDataServersInfoBean> list = zkBackupInfoInterface.loadDataServersInfoByClusterId(info.getId());
		map.put("list", list);
		return map;
	}
	
	
}
