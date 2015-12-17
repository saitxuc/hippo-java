package com.hippo.client.transport.cluster;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.TypeReference;
import com.hippo.client.ClientConstants;
import com.hippo.client.ClientSessionResult;
import com.hippo.client.HippoConnector;
import com.hippo.client.exception.HippoClientException;
import com.hippo.client.listener.ConnectionExceptionListener;
import com.hippo.client.schedule.ReprocessSchedule;
import com.hippo.client.transport.AbstractClientConnectionControl;
import com.hippo.client.util.ClientSPIManager;
import com.hippo.client.util.HippoClientUtil;
import com.hippo.client.util.ZkUtil;
import com.hippo.common.ZkConstants;
import com.hippo.common.domain.HippoClusterTableInfo;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.common.util.FastjsonUtil;
import com.hippo.network.Connection;
import com.hippo.network.ConnectionFactory;
import com.hippo.network.Session;
import com.hippo.network.impl.TransportConnectionFactory;

public class ClusterConnectionControl extends AbstractClientConnectionControl {

    private static final Logger log = LoggerFactory.getLogger(ClusterConnectionControl.class);

    private String zookeeperUrl;

    private String clusterName;
    
    private String userName;
    
    private String password;
    
    // key: brokerUrl, value: Connection
    private ConcurrentHashMap<String, Connection> connectionMap = new ConcurrentHashMap<String, Connection>();
    // key:brokerUrl, value: Semaphore
    private ConcurrentHashMap<String, Semaphore> semaphoreMap = new ConcurrentHashMap<String, Semaphore>();

    // hippo bucket-server table
    private Map<Integer, Vector<String>> table = null;

    private Vector<String> bucketBrokerUrls;
    
    public String getName() {
        return ClientConstants.TRANSPORT_PROTOCOL_CLUSTER;
    }

    @Override
    public void doInit() {
        ZkConstants.initClusterName(clusterName);
        bucketBrokerUrls = fetchBrokerUrlList();
        
        timingReconnectExecutor = ExcutorUtils.startSchedule("cluster reconnect schedule", 
                new ReprocessSchedule(this), 5*1000L, 30*1000L);
    }

    @Override
    public void doStart() {
        for (String brokerURL : bucketBrokerUrls) {
            if (!connectionMap.containsKey(brokerURL)) {
                Connection connection = null;
                try {
                    connection = createConnection(userName, password, replaceBrokerUrl(brokerURL));

                    if (connection != null) {
                        connection.start();
                        connection.addExceptionListener(new ConnectionExceptionListener(this, brokerURL));
                        createInitSessions(connection, brokerURL);
                        connectionMap.put(brokerURL, connection);
                        initSemaphore(brokerURL);
                    } else {
                        log.warn("init connection[" + brokerURL + "] happened error.");
                    }
                } catch (Throwable e) {
                    log.error("create Connection happened error, brokerUrl is + " + brokerURL, e);
                    if(connection != null) {
                        try {
                            connection.close();
                        } catch (HippoException e1) {
                        }
                    }
                    if(!badConnectionList.contains(brokerURL)) {
                        badConnectionList.add(brokerURL);
                    }
                }
            }
        }
        //startup listener for brokerUrl List change
        ClientSPIManager.startBrokerUrlListener(this);
    }

    @Override
    public void doStop() {
        for (Iterator<Entry<String, BlockingQueue<Session>>> iterator = sessionPoolMap.entrySet().iterator(); iterator.hasNext();) {
            Entry<String, BlockingQueue<Session>> entry = iterator.next();
            BlockingQueue<Session> sessionPool = entry.getValue();
            for (Session session : sessionPool) {
                try {
                    session.close();
                } catch (HippoException e) {
                    log.error("close session error. reason is: " + e.getMessage());
                }
            }
        }
        
        for (Iterator<Entry<String, Connection>> iterator = connectionMap.entrySet().iterator(); iterator.hasNext();) {
            Entry<String, Connection> entry = iterator.next();
            Connection connection = entry.getValue();
            try {
                connection.close();
            } catch (HippoException e) {
                log.error("close connection error. reason is" + e.getMessage());
            }
        }
        connectionMap.clear();
        sessionPoolMap.clear();
        if(timingReconnectExecutor != null) {
            timingReconnectExecutor.shutdown();
        }
    }

    public void createInitSessions(Connection connection, String brokerUrl) {
        BlockingQueue<Session> sessionPool = new LinkedBlockingQueue<Session>();
        for (int i = 0; i < sessionInstance; i++) {
            try {
                Session session = connection.createSession();
                sessionPool.offer(session);
            } catch (HippoException e) {
                log.error("create init Session happened error. ", e.getMessage());
            }
        }
        sessionPoolMap.put(brokerUrl, sessionPool);
    }
    
    public void addSessionForConnection(Connection connection, String brokerUrl) {
        BlockingQueue<Session> sessionPool = sessionPoolMap.get(brokerUrl);
        for (int i = 0; i < sessionInstance; i++) {
            try {
                Session session = connection.createSession();
                sessionPool.offer(session);
            } catch (HippoException e) {
                log.error("create init Session happened error. ", e.getMessage());
            }
        }
    }
    
    @Override
    public ClientSessionResult getSession(byte[] key) throws HippoClientException {
        if(bucketBrokerUrls.size() == 0) {
            throw new HippoClientException(" no broker available! ", HippoCodeDefine.HIPPO_CLIENT_SESSION_ERROR);
        }
        
        int bucket_num = HippoClientUtil.distributeBucket(key, bucketBrokerUrls.size());
        String brokerUrl = bucketBrokerUrls.get(bucket_num);

        BlockingQueue<Session> sessionPool = sessionPoolMap.get(brokerUrl);
        //check bad connection
        if(sessionPool == null && badConnectionList.contains(brokerUrl)) {
            throw new HippoClientException("brokerUrl["+ replaceBrokerUrl(brokerUrl) +"] is a bad connection.please wait reconnection success!", 
                    HippoCodeDefine.HIPPO_CONNECTION_FAILURE);
        }
        //check session num
        if (!hasCapacity(brokerUrl)) {
            throw new HippoClientException("session pool has full in cluster model! brokerUrl is ["+ brokerUrl +"]", 
                    HippoCodeDefine.HIPPO_CLIENT_SESSION_ERROR);
        }
        
        Session session = null;
        try {
            session = sessionPool.poll(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("get session from pool happened error in cluster model .", e.getMessage());
        }

        //create session
        if (session == null && connectionMap != null) {
            try {
                Connection connection = connectionMap.get(brokerUrl);
                if (connection != null) {
                    session = connection.createSession();
                }
            } catch (HippoException e) {
                throw new HippoClientException("create session happened error in cluster model! brokerUrl is ["+ brokerUrl +"]", 
                        HippoCodeDefine.HIPPO_CLIENT_SESSION_ERROR);
            }
        }
        return new ClientSessionResult(session, String.valueOf(bucket_num));
    }

    @Override
    public void offerSession(byte[] key, Session session) {
        String brokerUrl ="";
        try {
            if (session != null) {
                brokerUrl = bucketBrokerUrls.get(HippoClientUtil.distributeBucket(key, bucketBrokerUrls.size()));
                BlockingQueue<Session> sessionPool = sessionPoolMap.get(brokerUrl);
                if (sessionPool == null) {
                    return;
                }

                try {
                    if (!sessionPool.offer(session, ClientConstants.SESSION_POOL_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        try {
                            session.close();
                        } catch (HippoException e) {
                            log.error(e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("offer session happened error! ", e);
                }
            }
        } finally {
            returnCapacity(brokerUrl);
        }
    }

    public boolean hasCapacity(String brokerUrl) {
        Semaphore sessionSemaphore = semaphoreMap.get(brokerUrl);
        if (sessionSemaphore == null) {
            log.warn("get semaphore null from map! brokerUrl is [" + brokerUrl + "]");
        }

        if (sessionSemaphore != null) {
            try {
                if (!sessionSemaphore.tryAcquire(5, TimeUnit.MILLISECONDS)) {
                    return false;
                }
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    public void returnCapacity(String brokerUrl) {
        Semaphore sessionSemaphore = semaphoreMap.get(brokerUrl);
        if (sessionSemaphore != null) {
            sessionSemaphore.release();
        }
    }

    @Override
    public void reconnectionSchedule() {
        if(this.stoped.get()) {
            log.info(" Cluster connector controller has already stop, no need reconnection ");
            return;
        }
        for(String brokerUrl : badConnectionList) {
            if(!bucketBrokerUrls.contains(brokerUrl)) {
                badConnectionList.remove(brokerUrl);
                continue;
            }
            Connection connection = null;
            try {
                connection = createConnection(userName, password, replaceBrokerUrl(brokerUrl));
                
                if (connection != null) {
                    connection.start();
                    connection.addExceptionListener(new ConnectionExceptionListener(this, brokerUrl));
                    createInitSessions(connection, brokerUrl);
                    connectionMap.put(brokerUrl, connection);
                    badConnectionList.remove(brokerUrl);
                    initSemaphore(brokerUrl);
                }
            } catch (HippoException e) {
                log.error("brokerUrl["+ brokerUrl +"] reconnect error!", e);
            }
        }
    }
    
    public void disposeBrokerUrlListChange(String newBrokerUrlList) {
        if(this.stoped.get()) {
            log.info("Cluster connector controler has already stop. ");
            return;
        }
        if (log.isInfoEnabled()) {
            log.info("brokerUrl List happend change and dispose it. ");
        }

        timingReconnectExecutor.shutdown();
        
        if (!StringUtils.isEmpty(newBrokerUrlList)) {
            HippoClusterTableInfo clusterTableInfo = FastjsonUtil.jsonToObj(newBrokerUrlList, new TypeReference<HippoClusterTableInfo>() {
            });

            Vector<String> newBucketInfo = configBrokerUrl(clusterTableInfo.getTableMap().get(0));
            //close session and connection
            for (String brokerUrl : bucketBrokerUrls) {
                if (!newBucketInfo.contains(brokerUrl)) {
                    badConnectionList.remove(brokerUrl);
                    Connection connection = connectionMap.remove(brokerUrl);
                    BlockingQueue<Session> sessionPool = sessionPoolMap.remove(brokerUrl);
                    log.info("Close conncetion,brokerUrl is ["+ brokerUrl +"]");
                    if (connection != null) {
                        if (sessionPool != null) {
                            for (Session session : sessionPool) {
                                try {
                                    session.close();
                                } catch (Exception e) {
                                    log.error("close session happened error!", e);
                                }
                            }
                        }
                        
                        try {
                            connection.close();
                        } catch (Exception e) {
                            log.error("close connection happened error!", e);
                        }
                    }
                }
            }
            // create connection and sessions
            for (String brokerUrl : newBucketInfo) {
                if (!bucketBrokerUrls.contains(brokerUrl) && !connectionMap.containsKey(brokerUrl)) {
                    log.info("create Connection,brokerUrl is ["+ replaceBrokerUrl(brokerUrl) + "]");
                    Connection connection = null;
                    try {
                        connection = createConnection(userName, password, replaceBrokerUrl(brokerUrl));
                    } catch (HippoException e) {
                        log.error("create Connection happened error, brokerUrl is " + brokerUrl, e);
                        if(!badConnectionList.contains(brokerUrl)) {
                            badConnectionList.add(brokerUrl);
                        }
                    }
                    
                    if (connection != null) {
                        connection.start();
                        createInitSessions(connection, brokerUrl);
                        connectionMap.put(brokerUrl, connection);
                        initSemaphore(brokerUrl);
                    } else {
                        log.warn("create connection[" + brokerUrl + "] happened error when dispose brokerUrl List changes.");
                    }
                }
            }
            
            bucketBrokerUrls = newBucketInfo;
            timingReconnectExecutor = ExcutorUtils.startSchedule("cluster reconnect schedule", 
                    new ReprocessSchedule(this), 5*1000L, 30*1000L);
        }
    }

    /**
     * fecth brokerUrl list from zookeeper
     * 
     * @return
     */
    private Vector<String> fetchBrokerUrlList() {
        ZkClient zkClient = ZkUtil.getZKClient(zookeeperUrl);
        String brokerUrls = null;
        Vector<String> vector = new Vector<String>();
        String dataPath = ZkConstants.TABLES + ZkConstants.NODE_CTABLE;

        if (zkClient.exists(dataPath)) {
            brokerUrls = zkClient.readData(ZkConstants.TABLES + ZkConstants.NODE_CTABLE);
        }

        if(StringUtils.isEmpty(brokerUrls)) {
            log.warn("zookeeper node["+ dataPath +"] content null");
            return vector;
        }
        
        HippoClusterTableInfo clusterTableInfo = FastjsonUtil.jsonToObj(brokerUrls, new TypeReference<HippoClusterTableInfo>() {});
        table = clusterTableInfo.getTableMap();
        if (table.size() > 0) {
            vector = configBrokerUrl(table.get(0));
        }

        return vector;
    }

    private void initSemaphore(String brokerUrl) {
     if (!semaphoreMap.containsKey(brokerUrl)) {
            Semaphore sessionSemaphore = new Semaphore(sessionInstance);
            semaphoreMap.put(brokerUrl, sessionSemaphore);
        }
   }
    
    private Vector<String> configBrokerUrl(Vector<String> vector){
        Vector<String> v = new Vector<String>();
        for(int i=0; i<vector.size(); i++) {
            String url = vector.get(i);
            if(!"0".equals(url)) {
                v.add("hippo://" + url);
            }
        }
        return v;
    }
    
    private String replaceBrokerUrl(String brokerUrl) {
        return brokerUrl;
        //return brokerUrl.substring(brokerUrl.indexOf("_") + 1);
    }
    
    public void setConnector(HippoConnector connector) {
        //this.brokerUrl = connector.getBrokerUrl();
        this.sessionInstance = connector.getSessionInstance();
        this.zookeeperUrl = connector.getZookeeperUrl();
        this.clusterName = connector.getClusterName();
    }
    
    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public void setZookeeperUrl(String zookeeperUrl) {
        this.zookeeperUrl = zookeeperUrl;
    }

    @Override
    public Connection createConnection() throws HippoException {
         throw new RuntimeException("createConnection() not supported in cluster mode! ");
    }

    @Override
    public Connection createConnection(String userName, String password, String brokerUrl) throws HippoException {
        ConnectionFactory connectionFactory = new TransportConnectionFactory(brokerUrl);
        Connection connection = null;
        try{
            connection = connectionFactory.createConnection(userName, password);
        }catch(Exception e) {
            throw new HippoClientException("create connection happened error.", HippoCodeDefine.HIPPO_CONNECTION_FAILURE);
        }
        return connection;
    }

    @Override
    public void exceptionDispose(String brokerUrl) {
        if(!this.stoped.get()) {
            boolean isbad = false;
            try{
                Connection conn = connectionMap.remove(brokerUrl);
                BlockingQueue<Session> sessions = sessionPoolMap.remove(brokerUrl);
                semaphoreMap.remove(brokerUrl);
                if(sessions != null) {
                    for(Session session : sessions) {
                        if(session != null) {
                            session.close();
                        }
                    }
                }
                if(conn != null) {
                    isbad = true;
                    conn.close();
                }
                
            }catch(Exception e) {
                log.error("exception dispose dostop happen error! ", e);
            }finally {
                if(isbad) {
                    badConnectionList.add(brokerUrl);
                }
            }
        }
    }

}
