package com.hippo.client.util;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.CreateMode;

/**
 * 
 * @author DPJ
 * @version 2014-10-14
 */
public class ZookeeperUtil extends ZkUtil {

    // Cache my own client map
    protected static ConcurrentMap<String, ZkClient> zkClientMap = new ConcurrentHashMap<String, ZkClient>();
    protected static int connectTimeoutMs = ZkUtil.zkSessionTimeoutMs;
    
    public static ConcurrentMap<String, ZkClient> getZKClientMap(){
        return zkClientMap;
    }
    
    public static void setConnectionTimeout(int timeout) {
        connectTimeoutMs = timeout;
    }
    
    /**
     * Only support String data
     * @param zkAddress
     * @return ZkClient
     */
    public static ZkClient getZKClient(String zkAddress){
        ZkClient zkClient =zkClientMap.get(zkAddress);
        if(zkClient == null){
            ZKConfig zkConfig = new ZKConfig(zkAddress);
            try {
                zkClient = new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, 
                    connectTimeoutMs,new ZookeeperUtil.StringSerializer());
            } catch (ZkTimeoutException e) {
                throw new ZkTimeoutException("zookeeper address["+ zkConfig.zkConnect +"] connect timeout",e);
            }
            zkClientMap.put(zkAddress, zkClient);
        }
        return zkClient;
    }
    
    /**
     * 
     * @param zkAddress
     * @param path
     * @return boolean
     */
    public static boolean exist(final String zkAddress,final String path){
        return getZKClient(zkAddress).exists(path);
    }
    
    /**
     * 
     * @param zkAddress
     * @param path
     * @return List of children
     */
    public static List<String> getChildren(final String zkAddress,final String path){
        return getZKClient(zkAddress).getChildren(path);
    }

    /**
     * 
     * @param zkAddress
     * @param path
     * @return data
     */
    public static String getData(final String zkAddress,final String path){
        return getZKClient(zkAddress).readData(path);
    }
   
    /**
     * 
     * @param zkAddress
     * @param path
     * @param object
     */
    public static void setData(final String zkAddress, final String path, Object object) {
        getZKClient(zkAddress).writeData(path, object);
    }
    
    /**
     * 
     * @param zkAddress
     * @param path
     * @param data - Json formatted string
     * @param mode
     * @return path
     */
    public static String createNode(final String zkAddress, final String path, final String data, final CreateMode mode) {
        return getZKClient(zkAddress).create(path, data, mode);
    }
    
    /**
     * 
     * @param zkAddress
     * @param path
     * @param recursive
     * @return
     */
    public static boolean deleteNode(final String zkAddress, final String path, final boolean recursive) {
        if(recursive) {
            return getZKClient(zkAddress).deleteRecursive(path);
        } else {
            return getZKClient(zkAddress).delete(path);            
        }
    }
    
    public static void ensurePathExist(String zkAddress, String path, CreateMode createMode, boolean checkParentNode) {
        if (exist(zkAddress, path)) {
            return;
        }
        if (!checkParentNode) {
            if (!exist(zkAddress, path)) {
                createNode(zkAddress, path, "", createMode);
            }
        } else {
            String[] list = path.split("/");
            String zkPath = "";
            for (String str : list) {
                if (str.equals("") == false) {
                    zkPath = zkPath + "/" + str;
                    if (!exist(zkAddress, zkPath)) {
                        createNode(zkAddress, zkPath, "", createMode);
                    }
                }
            }
        }
    }
    
}
