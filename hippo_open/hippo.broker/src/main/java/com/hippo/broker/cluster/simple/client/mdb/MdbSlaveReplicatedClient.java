package com.hippo.broker.cluster.simple.client.mdb;

import static com.hippo.network.command.CommandConstants.COMMAND_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.client.SlaveReplicatedCommandManager;
import com.hippo.broker.cluster.coder.MdbCoderInitializer;
import com.hippo.broker.cluster.command.HeartBeatRequest;
import com.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.hippo.broker.cluster.simple.ZkRegisterService;
import com.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.hippo.common.NamedThreadFactory;
import com.hippo.common.SyncDataTask;
import com.hippo.common.SyncTaskTable;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.common.util.NetUtils;
import com.hippo.mdb.MdbMigrationEngine;
import com.hippo.network.Connection;
import com.hippo.network.ConnectionFactory;
import com.hippo.network.Session;
import com.hippo.network.command.Response;
import com.hippo.network.impl.TransportConnectionFactory;
import com.hippo.store.MigrationEngine;
import com.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 *
 */
public class MdbSlaveReplicatedClient extends ISlaveReplicatedClient {
    private static final Logger LOG = LoggerFactory.getLogger(MdbSlaveReplicatedClient.class);

    private ZkRegisterService registerService;

    private String masterUrl = null;

    private String username = null;

    private Serializer serializer = null;

    private String passwd = null;

    private Session session = null;

    private Connection connection = null;

    private MdbMigrationEngine migrationEngine;

    private ScheduledExecutorService heartBeatTimerService;

    private int replicated_port = 61100;

    private List<String> buckets = new ArrayList<String>();

    private ExecutorService jobExecutor = null;

    private String clientId;

    private ConcurrentHashMap<String, Long> bucketsSyncTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, Long> bucketsExpiredUpdatedTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, SyncTaskTable> taskTables = new ConcurrentHashMap<String, SyncTaskTable>();

    private ConcurrentHashMap<String, Long> lastRevResponseTime = new ConcurrentHashMap<String, Long>();

    private AtomicBoolean isStop = new AtomicBoolean(false);

    private ExecutorService syncRequestService = null;

    public MdbSlaveReplicatedClient() {
    }

    public MdbSlaveReplicatedClient(MigrationEngine migrationEngine, ZkRegisterService registerService, String replicated_port) {
        if (migrationEngine instanceof MdbMigrationEngine) {
            this.migrationEngine = (MdbMigrationEngine) migrationEngine;
        }

        this.registerService = registerService;
        if (StringUtils.isNotEmpty(replicated_port)) {
            this.replicated_port = Integer.parseInt(replicated_port);
        }
    }

    @Override
    public void doInit() {
        LOG.info("SlaveReplicatedClient begin to do init");
        this.masterUrl = registerService.getMasterUrl();
        ConnectionFactory connectionFactory = new TransportConnectionFactory(username, passwd, "recovery://" + masterUrl + ":" + replicated_port);
        connectionFactory.setCoderInitializer(new MdbCoderInitializer());
        connectionFactory.setCommandManager(new SlaveReplicatedCommandManager(this));

        try {
            connection = connectionFactory.createConnection();

            session = connection.createSession();
            clientId = connection.getClientID();
            migrationEngine.setDataTransCallBackList(null);
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
        for (BucketInfo info : migrationEngine.getBuckets()) {
            Integer bucket = info.getBucketNo();
            if (info.isSlave()) {
                buckets.add(bucket + "");
            }
        }
        LOG.info("SlaveReplicatedClient finished doing init");
    }

    /**
     * @param bucketNo
     * @param isSyncData
     * @param isVerifyExpire
     */
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
            LOG.info("processSyncTimeResponse | do not need sync with the server, client id -> " + clientId);
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

    private void processVerifyExpireResponse(Response response, SyncTaskTable taskTable) {
        byte[] data = response.getData();
        String bucketNo = response.getHeadValue("bucketNo");
        String originalVerifySize = response.getHeadValue("originalVerifySize");
        List<SyncDataTask> delTasks = null;

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
                delTasks = serializer.deserialize(data, List.class);
            }
        } catch (Exception e) {
            LOG.error("processVerifyExpireResponse | Exception when processDeleteDbInfosResponse, bucketNo -> " + bucketNo, e);
            return;
        }

        if (delTasks != null && delTasks.size() > 0) {
            LOG.info("processVerifyExpireResponse | get the delete task for verify size -> " + delTasks.size() + ", bucket -> " + bucketNo);
            for (SyncDataTask task : delTasks) {
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
                    throw new HippoStoreException(e.getMessage(), ((HippoStoreException) e).getErrorCode());
                }
            } else {
                throw new HippoStoreException(e.getMessage(), HippoCodeDefine.HIPPO_SERVER_ERROR);
            }
        }
    }

    @Override
    public void doStart() {
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
                long latestTime = migrationEngine.getBucketLatestModifiedTime(Integer.parseInt(bucket), false);
                bucketsSyncTime.putIfAbsent(bucket, latestTime);
                LOG.info("init the slave bucket latest time for the bucket " + bucket + ", the time is " + latestTime);
                bucketsExpiredUpdatedTime.putIfAbsent(bucket, 0L);
                lastRevResponseTime.put(bucket, 0L);
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
            heartBeatTimerService.shutdownNow();
        }

        if (syncRequestService != null) {
            syncRequestService.shutdownNow();
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
                        }
                    }

                    if (syncTimeMap.size() != 0 || expireUpdatedMap.size() != 0) {
                        HeartBeatRequest request = new HeartBeatRequest();
                        request.setAction(ReplicatedConstants.HEART_BEAT_ACTION);
                        request.setClientId(clientId);
                        request.putHeadValue(COMMAND_ID, "-1");
                        request.setSyncTime(syncTimeMap);
                        request.setExpireUpdatedTime(expireUpdatedMap);

                        try {
                            session.asnysend(request);
                            LOG.info("the heart beat sent message [syncTimeMap->" + syncTimeMap + "],[expireUpdatedMap->" + expireUpdatedMap + "]");
                        } catch (HippoException e) {
                            LOG.error("heart beat error!! client ip->" + NetUtils.getLocalAddress() + ", master url ->" + masterUrl, e);
                        }
                    }
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

    private void emergencyVerifyBucket(String bucketNo, SyncTaskTable taskTable) {
        LinkedList<SyncDataTask> tasks;
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
        for (Entry<String, Set<String>> entry : dataMap.entrySet()) {
            String sizeFlag = entry.getKey();
            Set<String> dbIfs = entry.getValue();
            if (dbIfs != null) {
                for (String dbIfId : dbIfs) {
                    SyncDataTask task = new SyncDataTask(dbIfId, sizeFlag);
                    list.add(task);
                }
            }
        }
        return list;
    }

}
