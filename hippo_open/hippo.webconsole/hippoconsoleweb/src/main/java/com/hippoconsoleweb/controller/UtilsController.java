package com.hippoconsoleweb.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hippoconsoleweb.service.ZkManageInterface;

@Controller
@RequestMapping("utils")
public class UtilsController {
	
	@Resource
	private ZkManageInterface zkManageInterface;
	

	@RequestMapping(value={"/clusterMenuZk"} , method=RequestMethod.POST)
	public @ResponseBody List<Map<String, String>> clusterMenuZk(ModelMap model)throws Exception{
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		list = zkManageInterface.loadCluseterMenuZk();
		return list;
	}
	
	@RequestMapping(value={"/clusterMenuBase"} , method=RequestMethod.POST)
	public @ResponseBody List<Map<String, String>> clusterMenuBase(ModelMap model)throws Exception{
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		list = zkManageInterface.loadCluseterMenuBase();
		return list;
	}
	
	@RequestMapping(value={"/clusterMenuBaseSelected"} , method=RequestMethod.POST)
	public @ResponseBody List<Map<String, String>> clusterMenuBaseSelected(ModelMap model)throws Exception{
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		list = zkManageInterface.loadCluseterMenuBaseSelected();
		return list;
	}
	
	
	
	@RequestMapping(value={"/serverMenuZk"} , method=RequestMethod.POST)
	public @ResponseBody List<Map<String, String>> serverMenuZk(ModelMap model,String clusterName)throws Exception{
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		list = zkManageInterface.loadServerMenuZk(clusterName);
		return list;
	}
	
	
	
}
