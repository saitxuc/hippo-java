package com.pinganfu.hippoconsoleweb.util;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author DPJ
 * @version 2014-10-14
 */
public class ZkUtils {
    protected static int zkSessionTimeoutMs = 30000;
    protected static int zkConnectTimeoutMs = 30000;

    // Cache my own client map
    protected static ConcurrentMap<String, ZkClient> zkClientMap = new ConcurrentHashMap<String, ZkClient>();

    public static ConcurrentMap<String, ZkClient> getZKClientMap() {
        return zkClientMap;
    }

    public static void setZkConnectionTimeoutMs(int timeout) {
        zkConnectTimeoutMs = timeout;
    }

    public static void setZkSessionTimeoutMs(int timeout) {
        zkSessionTimeoutMs = timeout;
    }

    /**
     * Only support String data
     * @param zkAddress
     * @return ZkClient
     */
    public static ZkClient getZKClient(String zkAddress) {
        ZkClient zkClient = zkClientMap.get(zkAddress);
        if (zkClient == null) {
            try {

                zkClient = new ZkClient(zkAddress, zkSessionTimeoutMs, zkConnectTimeoutMs, new ZkUtils.StringSerializer());

            } catch (ZkTimeoutException e) {
                throw new ZkTimeoutException("zookeeper address[" + zkAddress + "] connect timeout", e);
            }
            zkClientMap.put(zkAddress, zkClient);
        }
        return zkClient;
    }
    
    public static void createEphemeralPath(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            client.createEphemeral(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }
    
    public static void createParentPath(final ZkClient client, final String path) throws Exception {
        final String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }

    /**
     * 
     * @param zkAddress
     * @param path
     * @return boolean
     */
    public static boolean exist(final String zkAddress, final String path) {
        return getZKClient(zkAddress).exists(path);
    }

    /**
     * 
     * @param zkAddress
     * @param path
     * @return List of children
     */
    public static List<String> getChildren(final String zkAddress, final String path) {
        return getZKClient(zkAddress).getChildren(path);
    }

    /**
     * Get children of node, return empty list if null
     * @param zkAddress
     * @param path
     * @return List of children
     */
    public static List<String> getChildrenSafe(final String zkAddress, final String path) {
        List<String> children = getChildren(zkAddress, path);
        if (null == children) {
            children = new ArrayList<String>();
        }
        return children;
    }

    /**
     * 
     * @param zkAddress
     * @param path
     * @return data
     */
    public static String getData(final String zkAddress, final String path) {
        try {
            return getZKClient(zkAddress).readData(path);
        } catch(Exception e) {
        }
        return null; 
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
        if (recursive) {
            return getZKClient(zkAddress).deleteRecursive(path);
        } else {
            return getZKClient(zkAddress).delete(path);
        }
    }

    public static void ensurePathExist(String zkAddress, String path, CreateMode createMode, boolean checkParentNode) {
        if (ZkUtils.exist(zkAddress, path)) {
            return;
        }
        if (!checkParentNode) {
            if (!ZkUtils.exist(zkAddress, path)) {
                ZkUtils.createNode(zkAddress, path, "", createMode);
            }
        } else {
            String[] list = path.split("/");
            String zkPath = "";
            for (String str : list) {
                if (str.equals("") == false) {
                    zkPath = zkPath + "/" + str;
                    if (!ZkUtils.exist(zkAddress, zkPath)) {
                        ZkUtils.createNode(zkAddress, zkPath, "", createMode);
                    }
                }
            }
        }
    }

    /**
     * Get zookeeper
     * @param zkAddress
     * @return
     * @throws IOException
     */
    public static ZooKeeper fetchZkInstance(final String zkAddress) throws IOException {
        ZooKeeper zookeeper = new ZooKeeper(zkAddress, zkSessionTimeoutMs, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
            }

        });

        zookeeper.addAuthInfo("digest", "user:password".getBytes());
        return zookeeper;
    }

    public static class StringSerializer implements ZkSerializer {

        @Override
        public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
            try {
                return new String(bytes, "utf-8");
            } catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }

        @Override
        public byte[] serialize(final Object data) throws ZkMarshallingError {
            try {
                return ((String) data).getBytes("utf-8");
            } catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }

    }

    public static class ZKConfig implements Serializable {
        static final long serialVersionUID = -1L;

        public static String zkRoot = "/";
        /**
         * If enable zookeeper
         */
        public boolean zkEnable = true;

        /** ZK host string */
        public String zkConnect;

        /** zookeeper session timeout */
        public int zkSessionTimeoutMs = 30000;

        /**
         * the max time that the client waits to establish a connection to
         * zookeeper
         */
        public int zkConnectionTimeoutMs = 30000;

        /** how far a ZK follower can be behind a ZK leader */
        public int zkSyncTimeMs = 5000;

        public ZKConfig(final String zkConnect) {
            super();
            this.zkConnect = zkConnect;
        }

        public ZKConfig(final String zkConnect, final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs, final int zkSyncTimeMs) {
            super();
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
        }

        public ZKConfig() {
            super();
        }

        public ZKConfig(final String zkRoot, final String zkConnect, final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs, final int zkSyncTimeMs, final boolean zkEnable) {
            super();
            ZKConfig.zkRoot = zkRoot;
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
            this.zkEnable = zkEnable;
        }
    }

}
