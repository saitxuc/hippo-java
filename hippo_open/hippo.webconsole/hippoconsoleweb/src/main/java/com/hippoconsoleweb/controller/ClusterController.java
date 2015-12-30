package com.hippoconsoleweb.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hippo.client.HippoResult;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.util.FastjsonUtil;
import com.hippoconsoleweb.cmd.client.HippoClientCallInterface;
import com.hippoconsoleweb.common.LoadPropertiesData;
import com.hippoconsoleweb.model.ClusterInfoBean;
import com.hippoconsoleweb.model.CmdModel;
import com.hippoconsoleweb.service.ClusterInfoInterface;
import com.hippoconsoleweb.service.ZkManageInterface;
 
@Controller
@RequestMapping("cluster")
public class ClusterController {

	@Resource
	private LoadPropertiesData loadPropertiesData;
	@Resource
	private ClusterInfoInterface clusterInfoInterface;
	@Resource
	private ZkManageInterface zkManageInterface;
	@Resource
	private HippoClientCallInterface hippoClientCallInterface;
	
	private static KryoSerializer serializer = new KryoSerializer();
	
	private static Logger logger = LoggerFactory.getLogger(ClusterController.class);
	
	@RequestMapping(value = {"/clusterList"}, method = RequestMethod.GET)
	public String index(ModelMap model,String name) throws Exception{
        return "/cluster/cluster_list";
    }
	
	@RequestMapping(value={"/loadClusterList"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> loadClusterList(ModelMap model,ClusterInfoBean info,int size ,int page)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		info = this.getClusterInfoToPage(info,page,size);
		try{
			List<ClusterInfoBean> list = clusterInfoInterface.loadClusterInfoList(info);
			int total = clusterInfoInterface.findClusterInfoCount(info);
			map.put("total", total);
			map.put("rows", list);
		}catch(Exception ex){
			logger.error("load cluster list is error ;"+ex.toString());
		}
		
		return map;
	}
	
	@RequestMapping(value={"/viewCluster"} , method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> viewCluster(ModelMap model,ClusterInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		try{
			ClusterInfoBean view = clusterInfoInterface.findClusterInfo(info);
			map.put("clusterView", view);
		}catch(Exception ex){
			logger.error("load cluster view is error ;"+ex.toString());
		}
		return map;
	}
	
	@RequestMapping(value={"/addCluster"},method=RequestMethod.POST)
	public @ResponseBody Map<String, Object> addCluster(ClusterInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		int success = 0;
		try{
			ClusterInfoBean clusterInfo = new ClusterInfoBean();
			clusterInfo.setDf(0);
			clusterInfo.setClusterName(info.getClusterName());
			int issame = clusterInfoInterface.findClusterInfoCount(clusterInfo);
			if(issame  > 0){
				success = 2;
			}else{
				info.setStatus(1);
				int rows = clusterInfoInterface.insertClusterInfo(info);
				if(rows > 0){
					success = 1;
				}
			}
		}catch(Exception ex){
			logger.error("insert cluster error:"+ex.toString());
			success = 0;
		}
		map.put("success", success);
		return map;
	}
	
	@RequestMapping(value="/delCluster",method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> delCluster(ClusterInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		int success = 0;
		try{
			info.setDf(1);
			int rows = clusterInfoInterface.delClusterInfo(info);
			if(rows > 0){
				success = 1;
			}
		}catch(Exception ex){
			logger.error("del cluster error:"+ex.toString());
			success = 0;
		}
		map.put("success", success);
		return map;
	}
	
	@RequestMapping(value="/sendCluster",method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> sendCluster(ClusterInfoBean info)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		int success = 0;
		try{
			
			ClusterInfoBean infoN = clusterInfoInterface.findClusterInfo(info);
			boolean flag = zkManageInterface.sendClusterToZk(infoN);
			if(flag){
				infoN.setStatus(2);
				int rows = clusterInfoInterface.sendClusterInfo(infoN);
				if(rows > 0){
					success = 1;
				}
			}
		}catch(Exception ex){
			logger.error("send cluster error:"+ex.toString());
			success = 0;
			map.put("error", ex.toString());
		}
		map.put("success", success);
		return map;
	}
	
	
	@RequestMapping(value="/editCluster",method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> editCluster(ClusterInfoBean info,String oldClusterName)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		int success = 0;
		try{
			ClusterInfoBean clusterInfo = new ClusterInfoBean();
			clusterInfo.setDf(0);
			clusterInfo.setClusterName(info.getClusterName());
			int issame = clusterInfoInterface.findClusterInfoCount(clusterInfo);
			if(issame  > 0 && !oldClusterName.equals(info.getClusterName())){
				success = 2;
			}else{
				info.setStatus(1); //--默认设定为未推送状态
				int rows = clusterInfoInterface.editClusterInfo(info);
				if(rows > 0){
					success = 1;
				}
			}
			
		}catch(Exception ex){
			logger.error("edit cluster error:"+ex.toString());
			success = 0;
		}
		map.put("success", success);
		return map;
	}
	
	@RequestMapping(value="/cmdCluster",method=RequestMethod.POST)
	public @ResponseBody Map<String,Object> cmdCluster(HttpServletRequest request,CmdModel model)throws Exception{
		Map<String,Object> map = new HashMap<String, Object>();
		HippoResult result = null;
		
//		HttpSession session = request.getSession();
//		String userName = (String)session.getAttribute(LoadPropertiesData.USER_SESSION_KEY);
//		if(userName !=null && !userName.equals("admin")){
//			map.put("success", "4");
//			return map;
//		}
		
		if(model!=null && model.getCmdType() !=null && model.getCmdType().equals("1")){
			 result = hippoClientCallInterface.get(model.getKey(),model.getClusterName());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("2")){
			 result = hippoClientCallInterface.remove(model.getKey(), model.getClusterName());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("3")){
			 result = hippoClientCallInterface.inc(model.getKey(), model.getClusterName(), model.getVal(), model.getDefaultVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("4")){
			 result = hippoClientCallInterface.decr(model.getKey(), model.getClusterName(), model.getVal(), model.getDefaultVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("5")){
			 result = hippoClientCallInterface.set(model.getKey(), model.getClusterName(), model.getVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("6")){
			 result = hippoClientCallInterface.sset(model.getKey(), model.getClusterName(), model.getVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("7")){
			 result = hippoClientCallInterface.hset(model.getKey(), model.getClusterName(), model.getVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("8")){
			 result = hippoClientCallInterface.lset(model.getKey(), model.getClusterName(), model.getVal(), model.getExpireTime());
			
		}else if (model.getCmdType() !=null && model.getCmdType().equals("9")){
			 result = hippoClientCallInterface.update(model.getKey(), model.getClusterName(), model.getVal(), model.getDefaultVal(), model.getExpireTime());
			
		}else{}
		
		map = this.getPrint(result, map);
		return map;
	}
	
	/**
	 * 返回result内容
	 * @param result
	 * @param map
	 * @return
	 */
	 private Map<String,Object> getPrint(HippoResult result,Map<String,Object> map) {
        if (result != null) {
            if (result.isSuccess()) {
            	map.put("success", "1");
            	if(result.getData()!=null){
            		Object object = result.getDataForObject(serializer, null);
                    map.put("dataType", object.getClass().getName());
                    map.put("dataContent", FastjsonUtil.objToJson(object));
                    if (result.getSttribute("version") != null) {
                    	map.put("dataVersion", result.getSttribute("version"));
                    }
            	} else{
            		map.put("dataContent", "无返回内容");
            	}
                
            } else {
            	map.put("success", "2");
            	map.put("responseCode", result.getErrorCode());
                if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST)) {
                	map.put("errorLog", "data not found in hippo!!");
                } else if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_OPERATION_VERSION_WRONG)) {
                	map.put("errorLog", "data version not right!!");
                }
            }
        } else {
        	map.put("success", "3");
        	map.put("errorLog", "could not get the hippo result!!!");
        }
        return map;
    }
	
	
	private ClusterInfoBean getClusterInfoToPage(ClusterInfoBean info,int page,int rows){
        int start = (page-1)*rows;
        info.setOffset(start);
        info.setRows(rows);
        return info;
    }
	
}
