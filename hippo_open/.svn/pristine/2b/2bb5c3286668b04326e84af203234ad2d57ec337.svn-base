package com.pinganfu.hippo.broker.cluster.controltable.client.mdb;

import static com.pinganfu.hippo.network.command.CommandConstants.COMMAND_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.HeartBeatRequest;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableReplicatedClient;
import com.pinganfu.hippo.broker.cluster.controltable.client.CtrlTableSlaveReplicatedCommandManager;
import com.pinganfu.hippo.common.NamedThreadFactory;
import com.pinganfu.hippo.common.SyncDataTask;
import com.pinganfu.hippo.common.SyncTaskTable;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.common.util.ExcutorUtils;
import com.pinganfu.hippo.common.util.ListUtils;
import com.pinganfu.hippo.mdb.MdbMigrationEngine;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.Session;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.impl.TransportConnectionFactory;
import com.pinganfu.hippo.network.transport.nio.coder.MdbCoderInitializer;
import com.pinganfu.hippo.store.MigrationEngine;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 *
 */
public class MdbCtrlTableReplicatedClient extends ICtrlTableReplicatedClient {
    protected static final Logger LOG = LoggerFactory.getLogger(MdbCtrlTableReplicatedClient.class);

    protected String masterUrl = null;

    protected String username = null;

    protected Serializer serializer = null;

    protected String passwd = null;

    protected Session session = null;

    protected Connection connection = null;

    protected MdbMigrationEngine migrationEngine;

    protected ScheduledExecutorService heartBeatTimerService;

    protected String clientFlag = null;

    protected List<String> buckets = new ArrayList<String>();

    private ExecutorService jobExecutor = null;

    private String clientId;

    private ConcurrentHashMap<String, Long> bucketsSyncTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, Long> bucketsExpiredUpdatedTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, SyncTaskTable> taskTables = new ConcurrentHashMap<String, SyncTaskTable>();

    private ConcurrentHashMap<String, Long> lastRevResponseTime = new ConcurrentHashMap<String, Long>();

    private AtomicBoolean isStop = new AtomicBoolean(false);

    private ExecutorService syncRequestService = null;

    public MdbCtrlTableReplicatedClient(MigrationEngine migrationEngine, String masterUrl, List<String> buckets, String clientFlag) {
        if (migrationEngine instanceof MdbMigrationEngine) {
            this.migrationEngine = (MdbMigrationEngine) migrationEngine;
        }
        this.masterUrl = masterUrl;
        this.buckets = buckets;
        this.clientFlag = clientFlag;
    }

    @Override
    public void doInit() {
        LOG.info("MdbCtrlTableReplicatedClient begin to do init: {}:{}", masterUrl, clientFlag);
        ConnectionFactory connectionFactory = null;
        connectionFactory = new TransportConnectionFactory(username, passwd, "recovery://" + masterUrl);
        connectionFactory.setCoderInitializer(new MdbCoderInitializer());
        connectionFactory.setCommandManager(new CtrlTableSlaveReplicatedCommandManager(this));

        try {
            connection = connectionFactory.createConnection();
            connection.setClientID(clientFlag);
            session = connection.createSession();
            clientId = connection.getClientID();
        } catch (HippoException e) {
            e.printStackTrace();
            LOG.error(" Replicated Cluster client init happen. error: ", e);
        }

        if (serializer == null) {
            serializer = new KryoSerializer();
        }

        if (syncRequestService == null) {
            syncRequestService = ExcutorUtils.startSingleExcutor("syncRequestService");
        }

        migrationEngine.init();

        /// DPJ
        // buckets = storeEngine.getBuckets();
        LOG.info("MdbCtrlTableReplicatedClient finished doing init");
    }

    @Override
    public boolean resetBuckets(List<BucketInfo> resetBuckets) {
        //get remove list and added list from buckets and resetBuckets
        LOG.info("MdbCtrlTableReplicatedClient reset buckets -> " + resetBuckets + " begin!!");

        List<String> newSlaveBuckets = new ArrayList<String>();
        for (BucketInfo info : resetBuckets) {
            if (info.isSlave()) {
                newSlaveBuckets.add(info.getBucketNo() + "");
            }
        }

        //reset the thread pool for the client
        if (jobExecutor != null) {
            jobExecutor.shutdownNow();
            jobExecutor = ExcutorUtils.startPoolExcutor(newSlaveBuckets.size());
        }

        List<String> removeList = ListUtils.getDiffItemsFromSource(buckets, newSlaveBuckets);

        //unregisterToServer(removeList);

        for (String bucket : removeList) {
            bucketsSyncTime.remove(bucket);
            bucketsExpiredUpdatedTime.remove(bucket);
            taskTables.remove(bucket);
            lastRevResponseTime.remove(bucket);
        }

        for (String bucket : newSlaveBuckets) {
            bucketsSyncTime.putIfAbsent(bucket, 0L);
            bucketsExpiredUpdatedTime.putIfAbsent(bucket, 0L);
            lastRevResponseTime.putIfAbsent(bucket, 0L);
        }

        buckets.clear();
        buckets.addAll(newSlaveBuckets);

        for (String bucket : buckets) {
            sendSyncTimeWithThread(bucket, true, false);
        }

        LOG.info("MdbCtrlTableReplicatedClient reset buckets -> " + resetBuckets + " end!!");
        return true;
    }

    /**
     * send register info to the server
     * */
    /*public boolean registerToServer() {
        boolean success = false;
        RegisterRequest registerRequest = new RegisterRequest();
        registerRequest.setAction(ReplicatedConstants.REGISTER_RESPONSE_ACTION);
        registerRequest.setBucketsRequired(buckets);
        try {
            registerRequest.setClientId(clientId);
            Response rsp = session.send(registerRequest, 5 * 1000);
            if (rsp.isFailure()) {
                LOG.error("registerToServer | register failed!!!!!!!!!!!!!");
            } else {
                LOG.info("registerToServer | response got!! register successfully!!");
                success = true;
            }
        } catch (HippoException e) {
            LOG.error("Rmi error!", e);
        }
        return success;
    }*/

    /**
     * send unregister info to the server
     * */
    /*public void unregisterToServer(List<String> bucketsNo) {
        RegisterRequest registerRequest = new RegisterRequest();
        registerRequest.setAction(ReplicatedConstants.UNREGISTER_RESPONSE_ACTION);
        registerRequest.setBucketsRequired(bucketsNo);

        try {
            registerRequest.setClientId(clientId);
            Response rsp = session.send(registerRequest, 5 * 1000);
            if (rsp.isFailure()) {
                LOG.error("unregisterToServer | sending UnRegister failed!");
            } else {
                LOG.info("unregisterToServer | " + clientId + " unregister successgfully!! unregister buckets -> " + bucketsNo);
                for (String bucket : buckets) {
                    bucketsSyncTime.remove(bucket);
                    bucketsExpiredUpdatedTime.remove(bucket);
                    taskTables.remove(bucket);
                    lastRevResponseTime.remove(bucket);
                }
            }
        } catch (HippoException e) {
            LOG.error("Rmi error!", e);
        }
    }*/

    /**
     * send sendSyncTime to server
     * @param bucketNo
     * @param isSyncData
     * @param isVerifyExpire
     * */
    public void sendSyncTime2Server(String bucketNo, boolean isSyncData, boolean isVerifyExpire) {
        Long updatedTime = bucketsSyncTime.get(bucketNo);

        Long expiredUpdateTime = bucketsExpiredUpdatedTime.get(bucketNo);

        if (updatedTime == null || expiredUpdateTime == null) {
            LOG.error("sendSyncTime2Server | bucketNo -> " + bucketNo + " has been removed from the slave client! transactionState or bucketsSyncTime is null !!");
            return;
        }

        ReplicatedBucketRequest request = new ReplicatedBucketRequest();
        request.setAction(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION);
        request.setBuckNo(bucketNo);
        request.setClientId(clientId);

        if (isSyncData) {
            request.setModifiedTime(updatedTime);
        } else {
            request.setModifiedTime(ReplicatedConstants.NOT_NEED_SYNC);
        }

        if (isVerifyExpire) {
            request.setExpiredUpdateTime(expiredUpdateTime);
        } else {
            request.setExpiredUpdateTime(ReplicatedConstants.NOT_NEED_SYNC);
        }

        request.putHeadValue(COMMAND_ID, "-1");
        if (isSyncData || isVerifyExpire) {
            try {
                session.asnysend(request);
                LOG.info("sendSyncTime2Server | have sent the server to sync the time!! bucketNo -> " + bucketNo + ", client id -> " + clientId + ", isSyncData -> " + isSyncData + "[" + updatedTime + "], isVerifyExpire -> " + isVerifyExpire + "[" + expiredUpdateTime + "]");
            } catch (HippoException e) {
                LOG.error("sendSyncTime2Server | Rmi error!", e);
            }
        } else {
            LOG.info("sendSyncTime2Server | bucketNo -> " + bucketNo + " , client id -> " + clientId + " do not need to sync data!!");
        }
    }

    /**
     * 
     * fetch the sync data list from the server
     * @param response
     * @return
     */
    public void processSyncTimeResponse(Response response) {
        if (response.isFailure()) {
            LOG.warn("processSyncTimeResponse |  failure!!");
            return;
        }

        final String bucketNo = response.getHeadValue("bucketNo");
        //tasks
        SyncTaskTable taskTable = null;

        if (response.getData() != null) {
            try {
                taskTable = serializer.deserialize(response.getData(), SyncTaskTable.class);
            } catch (Exception e) {
                LOG.error("processSyncTimeResponse | Exception when deserialize in processNewSyncTimeAndDBInfos", e);
                return;
            }
        }

        if (taskTable == null) {
            LOG.error("processSyncTimeResponse | could not get the task table from the server ! bucket -> " + bucketNo);
            return;
        }

        if (taskTable.getEndExpiredTime() == ReplicatedConstants.NOT_NEED_SYNC && taskTable.getEndSyncTime() == ReplicatedConstants.NOT_NEED_SYNC) {
            LOG.info("processSyncTimeResponse | do not need sync with the server, client id -> " + clientId + ", bucketNo -> " + bucketNo);
            return;
        }

        synchronized (taskTable) {
            if (taskTable.getEndSyncTime() != ReplicatedConstants.NOT_NEED_SYNC) {
                taskTable.setSyncTasks(populateIntoTask(taskTable.getDbInfos()));
                LOG.info("processSyncTimeResponse | fetch the dbinfo list for the bucket -> " + bucketNo + ", newSyncTime -> " + taskTable.getEndSyncTime() + ", task size -> " + taskTable
                    .getSyncTasks().size());
            }

            if (taskTable.getEndExpiredTime() != ReplicatedConstants.NOT_NEED_SYNC) {
                try {
                    LinkedList<SyncDataTask> verifyExpireList = migrationEngine.getTasksNotExistedInSyncList(taskTable.getDbInfos(), bucketNo);
                    taskTable.setVerifyTasks(verifyExpireList);
                    LOG.info("processSyncTimeResponse | fetch the dbinfo list for the bucket -> " + bucketNo + ", newExpireVerifyTime -> " + taskTable
                        .getEndExpiredTime() + ", task size -> " + taskTable.getVerifyTasks().size());
                } catch (HippoStoreException e) {
                    if (e.getErrorCode().equals(HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED)) {
                        LOG.error("processSyncTimeResponse | HippoStoreException, bucket not existed!", e);
                    } else {
                        LOG.error("processSyncTimeResponse | HippoStoreException, error code is " + e.getErrorCode(), e);
                    }
                    return;
                }
            }
        }

        SyncTaskTable table = taskTables.putIfAbsent(bucketNo, taskTable);

        if (table == null) {
            //update the last rev time
            lastRevResponseTime.replace(bucketNo, System.currentTimeMillis());

            if (taskTable.isFinish()) {
                resetAfterFinish(bucketNo, taskTable);
            } else {
                //begin to do the sync
                processTaskInTransState(bucketNo);
            }
        } else {
            LOG.info("processSyncTimeResponse | old task existed, could not put new task table for the bucket -> " + bucketNo);
        }
    }

    private void sendSyncTimeWithThread(final String bucketNo, final boolean isNeedSyncData, final boolean isNeedVerifyExpire) {
        jobExecutor.execute(new Runnable() {
            @Override
            public void run() {
                sendSyncTime2Server(bucketNo, isNeedSyncData, isNeedVerifyExpire);
            }
        });
    }

    private void processTaskInTransState(final String bucketNo) {
        jobExecutor.execute(new Runnable() {
            @Override
            public void run() {
                final SyncTaskTable taskTable = taskTables.get(bucketNo);
                if (taskTable != null) {
                    ReplicatedBucketDataRequest request = new ReplicatedBucketDataRequest();
                    request.setAction(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION);
                    request.setBucketNo(bucketNo);
                    request.getHeaders().put("clientId", clientId);

                    boolean needSend = false;
                    synchronized (taskTable) {
                        List<SyncDataTask> verifyExpireList = taskTable.getVerifyTasks();
                        if (verifyExpireList != null && verifyExpireList.size() > 0) {
                            request.getHeaders().put("operation", "block_verify");
                            try {
                                List<SyncDataTask> subList = verifyExpireList.subList(0, (verifyExpireList.size() > ReplicatedConstants.VERIFY_TASK_SIZE ? ReplicatedConstants.VERIFY_TASK_SIZE : verifyExpireList
                                    .size()));
                                List<SyncDataTask> resultList = new ArrayList<SyncDataTask>(subList);
                                request.setData(serializer.serialize(resultList));
                                request.getHeaders().put("originalVerifySize", verifyExpireList.size() + "");
                                LOG.info("processTaskInTransState | detect the verify job , left number is " + verifyExpireList.size() + " , bucketNo -> " + bucketNo);
                            } catch (IOException e) {
                                LOG.error("processTaskInTransState | IOException when serialize verifyExpireList, bucketNo -> " + bucketNo, e);
                                return;
                            }
                            needSend = true;
                        } else {
                            LinkedList<SyncDataTask> syncList = taskTable.getSyncTasks();
                            if (syncList != null) {
                                SyncDataTask task = syncList.peekFirst();
                                if (task != null) {
                                    request.getHeaders().put("operation", "block_data");
                                    request.setDbinfo(task.getDbinfoId());
                                    request.setSizeFlag(task.getSizeFlag());
                                    needSend = true;
                                }
                            }
                        }
                    }

                    if (needSend) {
                        try {
                            session.asnysend(request);
                            //LOG.info("data has been sent to the server , bucket -> " + bucketNo + ", operation -> " + request.getHeadValue("operation"));
                        } catch (HippoException e) {
                            LOG.error("processTaskInTransState | HippoException when sending SyncDataTask, bucketNo -> " + bucketNo, e);
                        }
                    }
                } else {
                    LOG.error("processTaskInTransState | taskTables do not contain data for the bucket -> " + bucketNo);
                }
            }
        });
    }

    /**
     * process the sync data from the server
     * @param response
     */
    public void processSyncDataResponse(Response response) {
        String operation = response.getHeaders().get("operation");
        final String bucketNo = response.getHeaders().get("bucketNo");

        final SyncTaskTable taskTable = taskTables.get(bucketNo);
        if (taskTable == null) {
            LOG.warn("processSyncData | taskTable not existed, bucketNo -> " + bucketNo);
            return;
        }

        if (response.isFailure()) {
            LOG.warn("processSyncData | could not get the dbinfo data from the server!! bucket -> " + bucketNo);
            return;
        }

        lastRevResponseTime.replace(bucketNo, System.currentTimeMillis());

        try {
            if (operation.equals("block_verify")) {
                processVerifyExpireResponse(response, taskTable);
            } else if (operation.equals("block_data")) {
                processSyncBlockDataResponse(response, taskTable);
            } else {
                LOG.warn("processSyncData | no opertion defined -> " + operation + " in processSyncData!!");
            }

            //verify whether has finish all the task
            if (taskTable.isFinish()) {
                resetAfterFinish(bucketNo, taskTable);
            } else {
                processTaskInTransState(bucketNo);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void resetAfterFinish(String bucketNo, SyncTaskTable taskTable) {
        LOG.info("resetAfterFinish | detect the task for bucket -> " + bucketNo + " finished!!");
        if (taskTable.getEndSyncTime() != ReplicatedConstants.NOT_NEED_SYNC) {
            bucketsSyncTime.replace(bucketNo, taskTable.getEndSyncTime());
        }

        if (taskTable.getEndExpiredTime() != ReplicatedConstants.NOT_NEED_SYNC) {
            bucketsExpiredUpdatedTime.replace(bucketNo, taskTable.getEndExpiredTime());
        }

        boolean isVerifyExpire = false;

        taskTables.remove(bucketNo);

        try {
            if (migrationEngine.getUsedPercent(bucketNo) > 0.6) {
                //need to verify the expired
                isVerifyExpire = true;
            }
        } catch (HippoStoreException e) {
            LOG.error("resetAfterFinish | error when migrationEngine.getUsedPercent!", e);
            return;
        }

        sendSyncTimeWithThread(bucketNo, true, isVerifyExpire);
    }

    private void processVerifyExpireResponse(Response response, SyncTaskTable taskTable) {
        byte[] data = response.getData();
        String bucketNo = response.getHeadValue("bucketNo");
        String originalVerifySize = response.getHeadValue("originalVerifySize");
        List<SyncDataTask> delelteList = null;

        boolean isVerify = true;
        int size = 0;

        if (StringUtils.isEmpty(originalVerifySize)) {
            isVerify = false;
        } else {
            size = Integer.parseInt(originalVerifySize);
            synchronized (taskTable) {
                if (taskTable.getVerifyTasks().size() != size) {
                    isVerify = false;
                    LOG.warn("processVerifyExpireResponse | verify expire lists has been changed, this job will be ignore!! bucket -> " + bucketNo);
                    return;
                }
            }
        }

        if (!isVerify) {
            LOG.warn("processVerifyExpireResponse | could not verify the task! so will not process the response for the bucket -> " + bucketNo);
            return;
        }

        try {
            if (data != null) {
                delelteList = serializer.deserialize(data, List.class);
            }
        } catch (Exception e) {
            LOG.error("processVerifyExpireResponse | Exception when processDeleteDbInfosResponse, bucketNo -> " + bucketNo, e);
            return;
        }

        if (delelteList != null && delelteList.size() > 0) {
            LOG.info("processVerifyExpireResponse | get the delete task for verify size -> " + delelteList.size() + ", bucket -> " + bucketNo);
            for (SyncDataTask task : delelteList) {
                try {
                    migrationEngine.deleteDBIf(bucketNo, task.getSizeFlag(), task.getDbinfoId());
                } catch (Exception e) {
                    LOG.error("processVerifyExpireResponse | error when deleteDBIf for bucket -> " + bucketNo + ", dbinfo id -> " + task.getDbinfoId(), e);
                    return;
                }
            }
        }

        synchronized (taskTable) {
            if (taskTable.getVerifyTasks() != null && taskTable.getVerifyTasks().size() == size) {
                if (size < ReplicatedConstants.VERIFY_TASK_SIZE) {
                    taskTable.getVerifyTasks().clear();
                } else {
                    for (int i = 0; i < ReplicatedConstants.VERIFY_TASK_SIZE; i++) {
                        taskTable.getVerifyTasks().removeFirst();
                    }
                }
            }
        }
    }

    private void processSyncBlockDataResponse(Response response, SyncTaskTable taskTable) throws HippoStoreException {
        String bucketNo = response.getHeadValue("bucketNo");
        String dbinfo = response.getHeaders().get("dbinfo");
        String sizeFlag = response.getHeaders().get("sizeFlag");

        if (StringUtils.isEmpty(bucketNo) || StringUtils.isEmpty(sizeFlag) || StringUtils.isEmpty(dbinfo)) {
            throw new HippoStoreException("processSyncBlockDataResponse | bucketNo ,sizeFlag or dbinfo ,one of them is null", HippoCodeDefine.HIPPO_PARAM_NOT_RIGHT);
        }

        byte[] data = response.getData();
        try {
            SyncDataTask dbinfoInTask = taskTable.getSyncTasks().peekFirst();
            if (dbinfoInTask.getDbinfoId().equals(dbinfo)) {
                boolean isDone = false;
                if (data != null) {
                    boolean success = migrationEngine.replicated(bucketNo, sizeFlag, dbinfo, data);
                    if (success) {
                        LOG.info(String.format("fetch the data for dbinfo[%s],sizeFlag[%s],bucketNo[%s] and process sucessfully", dbinfo, sizeFlag, bucketNo));
                        isDone = true;
                    } else {
                        LOG.info(String.format("fetch the data for dbinfo[%s],sizeFlag[%s],bucketNo[%s] and process failed", dbinfo, sizeFlag, bucketNo));
                    }
                } else {
                    LOG.warn(String.format("data for dbinfo %s not found when transporting, it seems has been deleted! will delete it!!", dbinfo));
                    migrationEngine.deleteDBIf(bucketNo, sizeFlag, dbinfo);
                    isDone = true;
                }

                if (isDone) {
                    synchronized (taskTable) {
                        taskTable.getSyncTasks().remove(dbinfoInTask);
                    }
                }
            } else {
                LOG.warn("processSyncBlockDataResponse | dbinfo -> " + dbinfo + " in bucketNo -> " + bucketNo + " has been processed, this job will not do!!");
            }
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                if (((HippoStoreException) e).getErrorCode().equals(HippoCodeDefine.HIPPO_BUCKET_OUT_MEMORY)) {
                    //out of memory
                    emergencyVerifyBucket(bucketNo, taskTable);
                } else {
                    throw new HippoStoreException(((HippoStoreException) e).getMessage(), ((HippoStoreException) e).getErrorCode());
                }
            } else {
                throw new HippoStoreException(e.getMessage(), HippoCodeDefine.HIPPO_SERVER_ERROR);
            }
        }
    }

    private void emergencyVerifyBucket(String bucketNo, SyncTaskTable taskTable) {
        LinkedList<SyncDataTask> tasks = null;
        try {
            tasks = migrationEngine.emergencyVerifyBucket(bucketNo);
        } catch (HippoStoreException e) {
            LOG.error("emergencyVerifyBucket | HippoStoreException in emergencyVerifyBucket, error code is " + e.getErrorCode(), e);
            return;
        }

        if (tasks != null) {
            synchronized (taskTable) {
                taskTable.setVerifyTasks(tasks);
                LOG.info("emergencyVerifyBucket end! verify size is " + tasks.size() + " , bucket -> " + bucketNo);
            }
        } else {
            LOG.info("emergencyVerifyBucket end! could not fetch the verify task! bucket -> " + bucketNo);
        }
    }

    private LinkedList<SyncDataTask> populateIntoTask(Map<String, Set<String>> dataMap) {
        LinkedList<SyncDataTask> list = new LinkedList<SyncDataTask>();
        Iterator<Entry<String, Set<String>>> itertor = dataMap.entrySet().iterator();
        while (itertor.hasNext()) {
            Entry<String, Set<String>> entry = itertor.next();
            String sizeFlag = entry.getKey();
            Set<String> dbinfos = entry.getValue();
            if (dbinfos != null && dbinfos.size() > 0) {
                Iterator<String> dbIter = dbinfos.iterator();
                while (dbIter.hasNext()) {
                    String dbinfo = dbIter.next();
                    SyncDataTask task = new SyncDataTask(dbinfo, sizeFlag);
                    list.add(task);
                }
            }
        }
        return list;
    }

    @Override
    public void doStart() {
        LOG.info("--------- start slave for bucket: " + this.buckets + " from master: " + this.masterUrl);
        connection.start();
        isStop.set(false);

        //start the heart beat
        heartBeatTimerService = Executors.newScheduledThreadPool(3, new NamedThreadFactory("HeartBeatTimer", true));

        heartBeatReplicated();

        if (jobExecutor == null) {
            jobExecutor = ExcutorUtils.startPoolExcutor(buckets.size());
        }

        if (buckets != null) {
            for (String bucket : buckets) {
                bucketsSyncTime.putIfAbsent(bucket, 0L);
                bucketsExpiredUpdatedTime.putIfAbsent(bucket, 0L);
                lastRevResponseTime.put(bucket, 0L);
                LOG.info("do the state init for the bucket -> " + bucket);
                //trigger the time sync
                sendSyncTimeWithThread(bucket, true, false);
            }
        }
    }

    @Override
    public void doStop() {
        if (jobExecutor != null) {
            jobExecutor.shutdownNow();
            jobExecutor = null;
        }

        //unregisterToServer(buckets);

        if (heartBeatTimerService != null) {
            heartBeatTimerService.shutdown();
        }

        if (syncRequestService != null) {
            syncRequestService.shutdown();
        }

        isStop.set(true);

        try {
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (HippoException e) {
            e.printStackTrace();
            LOG.error(" Replicated Cluster client init happen. error: ", e);
        }

        bucketsSyncTime.clear();
        lastRevResponseTime.clear();
        bucketsExpiredUpdatedTime.clear();
        taskTables.clear();
    }

    private void heartBeatReplicated() {
        heartBeatTimerService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    Map<String, Long> syncTimeMap = new HashMap<String, Long>();
                    Map<String, Long> expireUpdatedMap = new HashMap<String, Long>();
                    long currentTime = System.currentTimeMillis();
                    Set<String> bucketsSent = new HashSet<String>();
                    for (String bucketNo : buckets) {
                        Long revRespTime = lastRevResponseTime.get(bucketNo);
                        Long syncTime = bucketsSyncTime.get(bucketNo);

                        if (revRespTime == null || syncTime == null) {
                            LOG.error("revRespTime or syncTime not init for the bucket -> " + bucketNo);
                            continue;
                        }

                        //data is out of date , limit it 3 minutes
                        if (currentTime - revRespTime > 60000) {
                            //remove the old Task
                            taskTables.remove(bucketNo);

                            //rety whole tasks
                            syncTimeMap.put(bucketNo, syncTime);
                            lastRevResponseTime.replace(bucketNo, System.currentTimeMillis());

                            if (migrationEngine.getUsedPercent(bucketNo) > 0.6) {
                                //need to verify the expired
                                expireUpdatedMap.put(bucketNo, bucketsExpiredUpdatedTime.get(bucketNo));
                            } else {
                                expireUpdatedMap.put(bucketNo, ReplicatedConstants.NOT_NEED_SYNC);
                            }

                            bucketsSent.add(bucketNo);
                        } else {
                            syncTimeMap.put(bucketNo, ReplicatedConstants.NOT_NEED_SYNC);
                            expireUpdatedMap.put(bucketNo, ReplicatedConstants.NOT_NEED_SYNC);
                        }
                    }

                    if (bucketsSent.size() != 0) {
                        HeartBeatRequest request = new HeartBeatRequest();
                        request.setAction(ReplicatedConstants.HEART_BEAT_ACTION);
                        request.setClientId(clientId);
                        request.putHeadValue(COMMAND_ID, "-1");
                        request.setSyncTime(syncTimeMap);
                        request.setExpireUpdatedTime(expireUpdatedMap);

                        try {
                            session.asnysend(request);
                            //LOG.info("the heart beat sent message [syncTimeMap->" + syncTimeMap + "],[expireUpdatedMap->" + expireUpdatedMap + "]");
                        } catch (HippoException e) {
                            LOG.error("heart beat error!! client id->" + clientId + ", master url ->" + masterUrl, e);
                        }
                    }
                    LOG.info("trigger heart beat [syncTimeMap->" + syncTimeMap + "] , [expireUpdatedMap->" + expireUpdatedMap + "]");
                } catch (Exception e) {
                    LOG.error("error when send the heartBeatReplicated, will retry later", e);
                }
            }
        }, 5, 30, TimeUnit.SECONDS);
    }

    public void processHeartBeatResponse(Response response) {
        if (response.isFailure()) {
            LOG.warn("heart beat response is failure!!");
            return;
        }

        Map<String, String> newDataUpdate = null;

        try {
            if (response.getData() != null) {
                newDataUpdate = serializer.deserialize(response.getData(), Map.class);
            }

            if (newDataUpdate != null && newDataUpdate.size() > 0) {
                for (String bucketNo : newDataUpdate.keySet()) {
                    String val = newDataUpdate.get(bucketNo);
                    String[] vals = val.split(",");
                    boolean isSyncData = Boolean.parseBoolean(vals[0]);
                    boolean isVerifyExpire = Boolean.parseBoolean(vals[1]);
                    LOG.info("processHeartBeatResponse | " + bucketNo + " need to sync with the server!! isSyncData,isVerifyExpire -> " + val);
                    //do it in the thread
                    sendSyncTimeWithThread(bucketNo, isSyncData, isVerifyExpire);
                }
            } else {
                String bucketsRequired = response.getHeadValue("bucketsRequired");
                LOG.info(bucketsRequired + " do not need to sync with the server!!");
            }
        } catch (IOException e) {
            LOG.error("processHeartBeatResponse | IOException", e);
        } catch (ClassNotFoundException e) {
            LOG.error("processHeartBeatResponse | ClassNotFoundException", e);
        } catch (InstantiationException e) {
            LOG.error("processHeartBeatResponse | InstantiationException", e);
        } catch (IllegalAccessException e) {
            LOG.error("processHeartBeatResponse | IllegalAccessException", e);
        }
    }

}
