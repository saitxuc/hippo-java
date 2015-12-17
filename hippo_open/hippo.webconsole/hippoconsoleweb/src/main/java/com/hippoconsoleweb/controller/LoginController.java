package com.hippoconsoleweb.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hippoconsoleweb.common.LoadPropertiesData;

@Controller
@RequestMapping("index")
public class LoginController {

	
	@RequestMapping(value = "/login", method = RequestMethod.GET)
    public String login(HttpServletRequest request, ModelMap model) throws Exception{
        return "/login";
    }
	
	@RequestMapping(value = "/logining")
    public @ResponseBody 
	Map<String, Object>  logining(HttpServletRequest request, ModelMap model,String username,String passWord)throws Exception{
    	Map<String,Object> map = new HashMap<String, Object>();
    	List<Map<String,String>> list = LoadPropertiesData.getUserList();
    	if(list !=null && list.size()>0){
    		int i= 0;
    		for(Map<String,String> userMap:list){
    			String uName = userMap.get("userName");
    			String uPwd = userMap.get("passwd");
    			if(username.equals(uName) && passWord.equals(uPwd)){
    				i++;
    			}
    		}
    		if(i > 0){
				map.put("returnValue", 2);
				map.put("userName", username);
				HttpSession session = request.getSession();
				session.removeAttribute(LoadPropertiesData.USER_SESSION_KEY);
				session.setAttribute(LoadPropertiesData.USER_SESSION_KEY, username);
			}else{
				map.put("returnValue", 1);
			}
    	}else{
    		map.put("returnValue", 0);
    	}
    	return map;
    }
	
	@RequestMapping(value = "/signup", method = RequestMethod.GET)
    public String signup(HttpServletRequest request, ModelMap model) throws Exception{
        return "/signup";
    }
	
	
	@RequestMapping(value = "/cluster", method = RequestMethod.GET)
    public String cluster(HttpServletRequest request, ModelMap model) throws Exception{
		HttpSession session = request.getSession();
		if(session.getAttribute(LoadPropertiesData.USER_SESSION_KEY) == null){
    		model.put("userName", "");
    		return "/login";
    	}else{
    		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
    		model.put("userName", userName);
    		return "/cluster";
    	}
    }
	
	@RequestMapping(value = "/dataService", method = RequestMethod.GET)
    public String dataService(HttpServletRequest request, ModelMap model) throws Exception{
		HttpSession session = request.getSession();
		if(session.getAttribute(LoadPropertiesData.USER_SESSION_KEY) == null){
    		model.put("userName", "");
    		return "/login";
    	}else{
    		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
    		model.put("userName", userName);
    		return "/dataService";
    	}
    }
	
	@RequestMapping(value = "/zookeeper", method = RequestMethod.GET)
    public String zookeeper(HttpServletRequest request, ModelMap model) throws Exception{
		HttpSession session = request.getSession();
		if(session.getAttribute(LoadPropertiesData.USER_SESSION_KEY) == null){
    		model.put("userName", "");
    		return "/login";
    	}else{
    		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
    		model.put("userName", userName);
    		return "/zookeeper";
    	}
    }
	
	@RequestMapping(value = "/zkBackUp", method = RequestMethod.GET)
    public String zkBackUp(HttpServletRequest request, ModelMap model) throws Exception{
		HttpSession session = request.getSession();
		if(session.getAttribute(LoadPropertiesData.USER_SESSION_KEY) == null){
    		model.put("userName", "");
    		return "/login";
    	}else{
    		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
    		model.put("userName", userName);
    		return "/zkBackUp";
    	}
    }
	
	@RequestMapping(value = "/machine", method = RequestMethod.GET)
    public String machine(HttpServletRequest request, ModelMap model) throws Exception{
		HttpSession session = request.getSession();
		if(session.getAttribute(LoadPropertiesData.USER_SESSION_KEY) == null){
    		model.put("userName", "");
    		return "/login";
    	}else{
    		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
    		model.put("userName", userName);
    		return "/machine";
    	}
    }
	
	@RequestMapping(value = "/modalIncludePage", method = RequestMethod.GET)
    public String modalIncludePage(HttpServletRequest request, ModelMap model) throws Exception{
        return "/modalIncludePage";
    }
	
	@RequestMapping(value = "/logout")
    public String logout(HttpServletRequest request,ModelMap model) throws Exception{
    	HttpSession session = request.getSession();
		session.removeAttribute(LoadPropertiesData.USER_SESSION_KEY);
        return "/login";
    }
	
	
	
	
}
