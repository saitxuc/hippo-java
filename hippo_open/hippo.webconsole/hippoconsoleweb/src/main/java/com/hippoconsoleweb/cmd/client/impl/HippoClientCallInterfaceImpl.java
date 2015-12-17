package com.hippoconsoleweb.cmd.client.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.hippo.client.DefaultHippoClient;
import com.hippo.client.HippoClient;
import com.hippo.client.HippoConnector;
import com.hippo.client.HippoResult;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippoconsoleweb.cmd.ClientManager;
import com.hippoconsoleweb.cmd.ClientPool;
import com.hippoconsoleweb.cmd.client.HippoClientCallInterface;
import com.hippoconsoleweb.common.LoadPropertiesData;

public class HippoClientCallInterfaceImpl implements HippoClientCallInterface {
    private Logger logger = LoggerFactory.getLogger(HippoClientCallInterfaceImpl.class);

    private ApplicationContext applicationContext;

    private static String zkAddress = LoadPropertiesData.getZkAddress();

    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    private ClientPool clientPool = null;

    public void setClientPool(ClientPool clientPool) {
        this.clientPool = clientPool;
    }

    public void startSheduledJob() {
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //loop pool manager
                HashSet<String> set = new HashSet<String>();
                set.addAll(clientPool.getPool().keySet());

                for (String name : set) {
                    ClientManager manager = clientPool.getPool().get(name);
                    if (manager != null && System.currentTimeMillis() - manager.getCreateTime() > 1800000) {
                        try {
                            manager.getClient().stop();
                            clientPool.getPool().remove(name);
                            logger.info("client -> " + name + " has been expired, will remove from the cache!!");
                        } catch (Exception e) {
                            logger.error("unexcepted error happened!! name is " + name, e);
                        }
                    }
                }
                set.clear();
                set = null;
            }
        }, 1, 5, TimeUnit.MINUTES);
    }

    @Override
    public HippoResult sset(String key, String clusterName, String val, String expireTime) {
        HippoClient client = getClient(clusterName);
        Serializable parsedKey = getSerializableObj(key);
        boolean setPermision = false;
        HippoResult result = client.get(parsedKey);
        if (!result.isSuccess()) {
            if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST)) {
                setPermision = true;
            }
        }

        if (setPermision) {
            try {
                int expireTimeParsed = Integer.parseInt(expireTime);
                Set set = new HashSet();
                String[] vals = val.split(",");
                for (String str : vals) {
                    Object data = getObject(str);
                    set.add(data);
                }
                result = client.set(expireTimeParsed, parsedKey, set, 0);
            } catch (NumberFormatException e) {
                logger.error("sset params not right, key[" + key + "],val[" + val + "],expireTimep[" + expireTime + "]", e);
                return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
            } catch (Exception e) {
                logger.error("unexpected error", e);
            }
        } else {
            result = new HippoResult(false, HippoCodeDefine.HIPPO_DATA_EXISTS, null);
        }

        return result;
    }

    @Override
    public HippoResult hset(String key, String clusterName, String val, String expireTime) {
        HippoClient client = getClient(clusterName);
        Serializable parsedKey = getSerializableObj(key);
        boolean setPermision = false;
        HippoResult result = client.get(parsedKey);
        if (!result.isSuccess()) {
            if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST)) {
                setPermision = true;
            }
        }

        if (setPermision) {
            try {
                int expireTimeParsed = Integer.parseInt(expireTime);
                Map map = new HashMap();

                String[] vals = val.split(",");

                for (String str : vals) {
                    String data[] = str.split(":");
                    map.put(getObject(data[0]), getObject(data[1]));
                }

                result = client.set(expireTimeParsed, parsedKey, map, 0);
            } catch (NumberFormatException e) {
                logger.error("hset params not right, key[" + key + "],val[" + val + "],expireTimep[" + expireTime + "]", e);
                return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
            } catch (Exception e) {
                logger.error("unexpected error", e);
            }
        } else {
            result = new HippoResult(false, HippoCodeDefine.HIPPO_DATA_EXISTS, null);
        }

        return result;
    }

    @Override
    public HippoResult lset(String key, String clusterName, String val, String expireTime) {
        HippoClient client = getClient(clusterName);
        Serializable parsedKey = getSerializableObj(key);
        boolean setPermision = false;
        HippoResult result = client.get(parsedKey);
        if (!result.isSuccess()) {
            if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST)) {
                setPermision = true;
            }
        }

        if (setPermision) {
            try {
                int expireTimeParsed = Integer.parseInt(expireTime);
                List list = new ArrayList();
                String[] vals = val.split(",");
                for (String str : vals) {
                    Object data = getObject(str);
                    list.add(data);
                }
                result = client.set(expireTimeParsed, parsedKey, list, 0);
            } catch (NumberFormatException e) {
                logger.error("lset params not right, key[" + key + "],val[" + val + "],expireTimep[" + expireTime + "]", e);
                return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
            } catch (Exception e) {
                logger.error("unexpected error", e);
            }
        } else {
            result = new HippoResult(false, HippoCodeDefine.HIPPO_DATA_EXISTS, null);
        }

        return result;
    }

    @Override
    public HippoResult set(String key, String clusterName, String val, String expireTime) {
        Serializable parsedKey = getSerializableObj(key);
        HippoClient client = getClient(clusterName);
        boolean setPermision = false;
        HippoResult result = client.get(parsedKey);
        if (!result.isSuccess()) {
            if (result.getErrorCode().equals(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST)) {
                setPermision = true;
            }
        }

        if (setPermision) {
            try {
                Serializable valParsed = getSerializableObj(val);
                int expireTimeParsed = Integer.parseInt(expireTime);
                result = client.set(expireTimeParsed, parsedKey, valParsed);
            } catch (NumberFormatException e) {
                logger.error("set params not right, key[" + key + "],val[" + val + "],expireTimep[" + expireTime + "]", e);
                return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
            } catch (Exception e) {
                logger.error("unexpected error", e);
            }
        } else {
            result = new HippoResult(false, HippoCodeDefine.HIPPO_DATA_EXISTS, null);
        }

        return result;
    }

    @Override
    public HippoResult update(String key, String clusterName, String val, String version, String expireTime) {
        throw new UnsupportedOperationException("update not supported now!!");
    }

    @Override
    public HippoResult remove(String key, String clusterName) {
        HippoClient client = getClient(clusterName);
        Serializable parsedKey = getSerializableObj(key);
        HippoResult result = client.remove(parsedKey);
        return result;
    }

    @Override
    public HippoResult get(String key, String clusterName) {
        HippoClient client = getClient(clusterName);
        Serializable parsedKey = getSerializableObj(key);
        HippoResult result = client.get(parsedKey);
        return result;
    }

    @Override
    public HippoResult inc(String key, String clusterName, String val, String defaultVal, String expireTime) {
        HippoResult result = null;
        try {
            HippoClient client = getClient(clusterName);
            long incValParsed = Long.parseLong(val);
            long defaultValParsed = Long.parseLong(defaultVal);
            int expireTimeParsed = Integer.parseInt(expireTime);
            result = client.incr(expireTimeParsed, getSerializableObj(key), incValParsed, defaultValParsed, true);
        } catch (NumberFormatException e) {
            logger.error("inc params not right, key[" + key + "],val[" + val + "],defaultVal[" + defaultVal + "],expireTimep[" + expireTime + "]", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
        } catch (Exception e) {
            logger.error("unexpected error", e);
        }
        return result;
    }

    @Override
    public HippoResult decr(String key, String clusterName, String val, String defaultVal, String expireTime) {
        HippoResult result = null;
        try {
            HippoClient client = getClient(clusterName);
            long incValParsed = Long.parseLong(val);
            long defaultValParsed = Long.parseLong(defaultVal);
            int expireTimeParsed = Integer.parseInt(expireTime);
            result = client.decr(expireTimeParsed, getSerializableObj(key), incValParsed, defaultValParsed, true);
        } catch (NumberFormatException e) {
            logger.error("decr params not right, key[" + key + "],val[" + val + "],defaultVal[" + defaultVal + "],expireTimep[" + expireTime + "]", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT, null);
        } catch (Exception e) {
            logger.error("unexpected error", e);
        }
        return result;
    }

    public Serializable getSerializableObj(String content) {
        Serializable object = null;

        if (isStr(content)) {
            object = content.replaceAll("\"", "").replaceAll("'", "");
        } else if (containAlph(content)) {
            object = content;
        } else {
            if (isLong(content)) {
                object = Long.parseLong(content.substring(0, content.length() - 1));
            } else if (isDouble(content)) {
                object = Double.parseDouble(content.substring(0, content.length() - 1));
            } else if (isFloat(content)) {
                object = Float.parseFloat(content.substring(0, content.length() - 1));
            } else if (defaultInt(content)) {
                object = Integer.parseInt(content);
            } else if (defaultFloat(content)) {
                object = Float.parseFloat(content);
            } else {
                logger.error("the key -> " + content + " could not be parsed!!");
            }
        }
        return object;
    }

    public Object getObject(String content) {
        Object object = null;

        if (isStr(content)) {
            object = content.replaceAll("\"", "").replaceAll("'", "");
        } else if (containAlph(content)) {
            object = content;
        } else {
            if (isLong(content)) {
                object = Long.parseLong(content.substring(0, content.length() - 1));
            } else if (isDouble(content)) {
                object = Double.parseDouble(content.substring(0, content.length() - 1));
            } else if (isFloat(content)) {
                object = Float.parseFloat(content.substring(0, content.length() - 1));
            } else if (defaultInt(content)) {
                object = Integer.parseInt(content);
            } else if (defaultFloat(content)) {
                object = Float.parseFloat(content);
            } else {
                logger.error("the key -> " + content + " could not be parsed!!");
            }
        }
        return object;
    }

    private HippoClient getClient(String clusterName) {
        HippoClient client = clientPool.getClient(clusterName);
        if (client == null) {
            HippoConnector cc = new HippoConnector();
            cc.setZookeeperUrl(zkAddress);
            cc.setClusterName(clusterName);
            cc.setSessionInstance(2);
            client = DefaultHippoClient.createClient(cc);
            client.start();
            if (!clientPool.putClientIfAbsent(clusterName, client)) {
                client.stop();
            }
        } else {
            if (!client.isStarted()) {
                clientPool.removeClient(clusterName);
                HippoConnector cc = new HippoConnector();
                cc.setZookeeperUrl(zkAddress);
                cc.setClusterName(clusterName);
                client = DefaultHippoClient.createClient(cc);
                client.start();
                if (!clientPool.putClientIfAbsent(clusterName, client)) {
                    client.stop();
                }
            }
        }
        return client;
    }

    private boolean containAlph(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean isStr(String data) {
        if (data.startsWith("\"") && data.endsWith("\"")) {
            return true;
        } else if (data.startsWith("'") && data.endsWith("'")) {
            return true;
        } else {
            return false;
        }
    }

    private boolean defaultInt(String data) {
        return data.matches("^[-]{0,1}[0-9]+$");
    }

    private boolean defaultFloat(String data) {
        return data.matches("^[-]{0,1}[0-9]+(.[0-9]+)?$");
    }

    private boolean isDouble(String data) {
        return data.matches("^[-]{0,1}[0-9]+(.[0-9]+)?[d|D]$");
    }

    private boolean isLong(String data) {
        return data.matches("^[-]{0,1}[0-9]+[l|L]$");
    }

    private boolean isFloat(String data) {
        return data.matches("^[-]{0,1}[0-9]+(.[0-9]+)?[f|F]$");
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

}
