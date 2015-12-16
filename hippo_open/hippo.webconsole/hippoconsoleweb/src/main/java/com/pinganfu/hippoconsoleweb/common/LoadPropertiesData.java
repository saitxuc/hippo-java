package com.pinganfu.hippoconsoleweb.common;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadPropertiesData {
	
	private static Logger logger = LoggerFactory.getLogger(LoadPropertiesData.class);
	public static String USER_SESSION_KEY = "userKey";
	public static String PERMISSION = "admin";
	public static String PROPERTIES_PATH = "/wls/wls81/envconfig/hippoconsoleweb/env.properties";
	

	public String getLinkPath(String key)throws Exception{
		Properties properties = new Properties();
    	FileInputStream in = new FileInputStream(PROPERTIES_PATH);
    	properties.load(in);
        //properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("localFile.properties"));
        String path = properties.getProperty(key);
		return path;
	}
	
	public static String getZkAddress(){
		Properties properties = new Properties();
        try {
        	FileInputStream in = new FileInputStream(PROPERTIES_PATH);
        	properties.load(in);
			//properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("env.properties"));
		} catch (Exception e) {
			logger.error("load properties error"+e.toString());
		}
        String address = properties.getProperty("dubbo.registry.address");
		return address;
	}
	
	public static List<Map<String,String>> getUserList()throws Exception{
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = null;
		if(1==1){
			map = new HashMap<String, String>();
			String userName = getLoginLinkPath("adminUser");
			String passwd = getLoginLinkPath("adminPassWd");
			map.put("userName", userName);
			map.put("passwd", passwd);
			list.add(map);
		}
		if(2==2){
			map = new HashMap<String, String>();
			String userName = getLoginLinkPath("guestUser");
			String passwd = getLoginLinkPath("guestPassWd");
			map.put("userName", userName);
			map.put("passwd", passwd);
			list.add(map);
		}
		
		return list;
	}
	
	
	public static boolean validatePermission (HttpServletRequest request){
		HttpSession session = request.getSession();
		String user = (String)session.getAttribute(USER_SESSION_KEY);
		if(user.equals(PERMISSION)){
			return true;
		}
		return false;
	}
	
	public static String getLoginLinkPath(String key)throws Exception{
		Properties properties = new Properties();
    	FileInputStream in = new FileInputStream(PROPERTIES_PATH);
    	properties.load(in);
        String path = properties.getProperty(key);
		return path;
	}
}
