package com.pinganfu.hippo.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.zookeeper.CreateMode;

import com.pinganfu.hippo.client.util.ZookeeperUtil;
import com.pinganfu.hippo.common.ZkConstants;
import com.pinganfu.hippo.common.domain.HippoClusterConifg;
import com.pinganfu.hippo.common.domain.HippoClusterTableInfo;
import com.pinganfu.hippo.common.util.FastjsonUtil;

public class InitZkData {

    public static final String zkAddress = "localhost:2181";

    public static void clear() {
        if (ZookeeperUtil.exist(zkAddress, ZkConstants.PATH_ROOT)) {
            ZookeeperUtil.deleteNode(zkAddress, ZkConstants.PATH_ROOT, true);
        }
    }
    
    public static void updateDtalbe() {
        Map<Integer, Vector<String>> ctable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(2, nodeV);

        HippoClusterTableInfo tableInfo = new HippoClusterTableInfo();
        tableInfo.setVersion(1);
        tableInfo.setTableMap(ctable);

        ZookeeperUtil.setData(zkAddress, ZkConstants.TABLES + ZkConstants.NODE_MTABLE, FastjsonUtil.objToJson(tableInfo));
    }

    public static void init() {

        ZookeeperUtil.ensurePathExist(zkAddress, ZkConstants.TABLES, CreateMode.PERSISTENT, true);

        HippoClusterConifg config = new HippoClusterConifg();
        config.setCopycount(2);
        config.setHashcount(4);
        config.setDbType("mdb");
        config.setName("hippo test");
        config.setReplicatePort(61102);
        config.setBucketsLimit("10");
        
        update(ZkConstants.CONFIG, FastjsonUtil.objToJson(config));

        Map<Integer, Vector<String>> ctable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(2, nodeV);

        HippoClusterTableInfo tableInfo = new HippoClusterTableInfo();
        tableInfo.setVersion(1);
        tableInfo.setTableMap(ctable);

        update(ZkConstants.TABLES + ZkConstants.NODE_CTABLE, null);
        update(ZkConstants.TABLES + ZkConstants.NODE_MTABLE, null);
        update(ZkConstants.TABLES + ZkConstants.NODE_DTABLE, null);
        update(ZkConstants.MIGRATION, null);
        update(ZkConstants.DATA_SERVERS, null);
    }
    
    private static void update(String path, String data) {
        if(ZookeeperUtil.exist(zkAddress, path)) {
            if(null == data) {
                data = "";
            }
            ZookeeperUtil.setData(zkAddress, path, data);
        } else {
            ZookeeperUtil.createNode(zkAddress, path, data, CreateMode.PERSISTENT);
        }
    }

    public static void main(String[] args) {
        String clusterName = "cluster_1";
        ZkConstants.initClusterName(clusterName);
        
        init();
        //ZookeeperUtil.createNode(zkAddress, ZkConstants.DATA_SERVERS + "/127.0.0.4:61300", "", CreateMode.PERSISTENT);

        Map<Integer, Vector<String>> ctable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.2:61300");
        nodeV.add("127.0.0.3:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.2:61300");
        ctable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.3:61300");
        nodeV.add("127.0.0.3:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.3:61300");
        nodeV.add("127.0.0.3:61300");
        ctable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("127.0.0.2:61300");
        nodeV.add("127.0.0.1:61300");
        nodeV.add("127.0.0.2:61300");
        nodeV.add("127.0.0.2:61300");
        nodeV.add("127.0.0.1:61300");
        ctable.put(2, nodeV);

        // ZookeeperUtil.setData(zkAddress, ZkConstants.TABLES + ZkConstants.NODE_CTABLE, FastjsonUtil.objToJson(ctable));
    }

}
