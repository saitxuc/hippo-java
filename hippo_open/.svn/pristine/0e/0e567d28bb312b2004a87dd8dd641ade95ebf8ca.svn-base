package com.pinganfu.hippo.broker.cluster.controltable.master.mdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.HeartBeatRequest;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.pinganfu.hippo.broker.cluster.controltable.CtrlTableClusterBrokerService;
import com.pinganfu.hippo.broker.cluster.controltable.ICtrlTableReplicatedServer;
import com.pinganfu.hippo.client.HippoResult;
import com.pinganfu.hippo.common.SyncDataTask;
import com.pinganfu.hippo.common.SyncTaskTable;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.common.util.ListUtils;
import com.pinganfu.hippo.mdb.MdbConstants;
import com.pinganfu.hippo.mdb.MdbMigrationEngine;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.ServerFortressFactory;
import com.pinganfu.hippo.network.impl.TransportServerFortressFactory;
import com.pinganfu.hippo.network.transport.nio.server.NioTransportConnectionManager;
import com.pinganfu.hippo.store.MigrationEngine;
import com.pinganfu.hippo.store.TransDataCallBack;

/**
 * 
 * @author saitxuc
 *
 */
public class MdbCtrlTableReplicatedServer extends ICtrlTableReplicatedServer {
    protected static final Logger LOG = LoggerFactory.getLogger(MdbCtrlTableReplicatedServer.class);

    private Serializer serializer = null;
    private NioTransportConnectionManager connectionManager;
    private ServerFortress serverFortress;
    private MdbMigrationEngine migrationEngine;
    private int replicated_port = 61100;

    private ConcurrentHashMap<String, Long> bucketsLatestUpTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, Long> bucketsExpiredUpdatedTime = new ConcurrentHashMap<String, Long>();

    //private Set<String> registerClientIds = new HashSet<String>();

    private CtrlTableClusterBrokerService broker;

    private List<String> bucketsList = new ArrayList<String>();

    private ConcurrentHashMap<String, Object> triggerReplicatedEvent = new ConcurrentHashMap<String, Object>();

    public MdbCtrlTableReplicatedServer(String replicated_port, CtrlTableClusterBrokerService broker) {
        this.broker = broker;
        if (StringUtils.isNotEmpty(replicated_port)) {
            this.replicated_port = Integer.parseInt(replicated_port);
        }
    }

    @Override
    public void doInit() {
        LOG.info("MasterReplicatedServer begin to do init");

        if (migrationEngine != null) {
            migrationEngine.init();

            List<TransDataCallBack> callbacks = new ArrayList<TransDataCallBack>();
            callbacks.add(new MdbUpdateBucketTime());
            migrationEngine.setDataTransCallBackList(callbacks);
        } else {
            LOG.warn("MdbCtrlTableReplicatedServer | doInit | migrationEngine is not set!!");
        }

        LOG.info("AsyncCallback was set successfully");

        if (serializer == null) {
            serializer = new KryoSerializer();
        }

        connectionManager = new NioTransportConnectionManager();

        ServerFortressFactory sfactory = new TransportServerFortressFactory(TransportServerFortressFactory.DEFAULT_SHEME, replicated_port, new MdbCtrlTableReplicatedCommandManager(this), connectionManager);
        sfactory.setNioType(TransportServerFortressFactory.DEFAULT_NIO_TYPE);
        sfactory.setConnectionManager(connectionManager);
        try {
            serverFortress = sfactory.createServer();
        } catch (HippoException e) {
            LOG.error("HippoException", e);
        }

        LOG.info("MasterReplicatedServer finished doing init");
    }

    @Override
    public void doStart() {
        LOG.info("MasterReplicatedServer begin to do doStart");
        try {
            if (serverFortress != null) {
                serverFortress.start();
            }

            //migrationEngine.startMdbStoreEnginePeriodsExpire();

            if (migrationEngine != null && migrationEngine.isStarted()) {
                List<BucketInfo> buckets = migrationEngine.getBuckets();

                if (buckets != null) {
                    for (BucketInfo info : buckets) {
                        if (!info.isSlave()) {
                            Integer bucketNo = info.getBucketNo();
                            long latestTime = migrationEngine.getBucketLatestModifiedTime(bucketNo, true);
                            LOG.info("init the bucket latest time for the bucket " + bucketNo + ", the time is " + latestTime);
                            bucketsList.add(bucketNo + "");
                            bucketsLatestUpTime.putIfAbsent(bucketNo + "", latestTime);
                            bucketsExpiredUpdatedTime.put(bucketNo + "", 0L);
                        }
                    }
                }
            } else {
                LOG.warn("MdbCtrlTableReplicatedServer | doStart | migrationEngine is not set!!");
            }
        } catch (Exception e) {
            LOG.error("master start failed!!", e);
        }
        LOG.info("MasterReplicatedServer finished doing doStart");
    }

    @Override
    public void doStop() {
        LOG.info("MasterReplicatedServer begin to do doStop");

        if (serverFortress != null) {
            serverFortress.stop();
        }

        if (migrationEngine != null) {
            migrationEngine.stopMdbStoreEnginePeriodsExpire();
        }

        LOG.info("MasterReplicatedServer finished doing doStop");
    }

    /**
     * process the register request,record the client and bucket relationship 
     * @param request
     * @return
     */
    /*public HippoResult processRegisterRequest(RegisterRequest request) {
        HippoResult result = new HippoResult(true);
        String clientId = request.getClientId();
        List<String> buckets = request.getBucketsRequired();

        if (StringUtils.isNotEmpty(clientId) && buckets != null) {
            boolean isError = false;
            registerClientIds.add(clientId);

            if (isError) {
                processUnRegisterRequest(request);
                result.setSuccess(false);
                result.setErrorCode(HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED);
            } else {
                LOG.info("processRegisterRequest | get register request, clientId -> " + clientId + " registered, the buckets -> " + buckets);
                result.setSuccess(true);
                result.setErrorCode(HippoCodeDefine.HIPPO_SUCCESS);
            }
        } else {
            LOG.warn("processRegisterRequest | buckets or clientid is null, failed do register request");
            result.setSuccess(false);
            result.setErrorCode(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
        }
        return result;
    }*/

    /* public HippoResult processUnRegisterRequest(RegisterRequest request) {
         HippoResult result = new HippoResult(true);
         String clientId = request.getClientId();
         List<String> buckets = request.getBucketsRequired();

         if (StringUtils.isNotEmpty(clientId) && buckets != null) {
             registerClientIds.remove(clientId);
             result.setSuccess(true);
             result.setErrorCode(HippoCodeDefine.HIPPO_SUCCESS);
         } else {
             result.setSuccess(false);
             result.setErrorCode(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
             LOG.info("processUnRegisterRequest | buckets or clientid is null, failed do unregister request");
         }
         return result;
     }*/

    /***
     * process the data sync list according the bucket number and time
     * 
     * @param request
     * @return
     */
    public HippoResult processSyncTimeRequest(ReplicatedBucketRequest request) {
        HippoResult hippoResult = new HippoResult(false);
        SyncTaskTable table = new SyncTaskTable();
        String bucketNo = request.getBuckNo();
        String clientId = request.getClientId();
        long modifiedTime = request.getModifiedTime();
        long expiredUpdateTime = request.getExpiredUpdateTime();
        hippoResult.putAttribute("bucketNo", bucketNo);
        try {

            if (migrationEngine == null || !migrationEngine.isStarted()) {
                table.setEndSyncTime(ReplicatedConstants.NOT_NEED_SYNC);
                table.setEndExpiredTime(ReplicatedConstants.NOT_NEED_SYNC);
                LOG.warn("processSyncTimeRequest | migrationEngine is not set, return sync success back!!");
            } else {
                boolean isNeedSyncExpire = false;
                if (expiredUpdateTime != ReplicatedConstants.NOT_NEED_SYNC) {
                    isNeedSyncExpire = isNeedSyncExpired(bucketNo, expiredUpdateTime);
                }

                boolean isNeedSync = false;
                if (modifiedTime != ReplicatedConstants.NOT_NEED_SYNC) {
                    isNeedSync = isNeedSyncData(bucketNo, modifiedTime);
                }

                if (isNeedSync) {
                    Long endTime = bucketsLatestUpTime.get(bucketNo);
                    if (endTime == null) {
                        LOG.warn("processSyncTimeRequest | bucket -> " + bucketNo + " is removed or not existed, please check!!");
                        return hippoResult;
                    }
                    Map<String, Set<String>> result = migrationEngine.collectLatestDbInfoIds(bucketNo, modifiedTime, endTime);
                    table.setDbInfos(result);
                    table.setEndSyncTime(endTime);
                } else {
                    table.setEndSyncTime(ReplicatedConstants.NOT_NEED_SYNC);
                }

                if (isNeedSyncExpire) {
                    Long endTime = bucketsExpiredUpdatedTime.get(bucketNo);
                    if (endTime == null) {
                        LOG.warn("processSyncTimeRequest | bucket -> " + bucketNo + " is removed or not existed, please check!!");
                        return hippoResult;
                    }
                    table.setEndExpiredTime(endTime);
                } else {
                    table.setEndExpiredTime(ReplicatedConstants.NOT_NEED_SYNC);
                }
            }

            if (table.getEndSyncTime() == ReplicatedConstants.NOT_NEED_SYNC) {
                LOG.info("processSyncTimeRequest | clientId->" + clientId + " data has sync the changed data with the server, sync time -> " + modifiedTime + " , but expire not verify! bucketNo -> " + bucketNo);
                String identify = bucketNo + "-" + clientId;
                if (!triggerReplicatedEvent.contains(identify)) {
                    Object oldValue = triggerReplicatedEvent.putIfAbsent(identify, new Object());
                    if (oldValue == null) {
                        this.whenFinishedReplicateBucket(bucketNo, clientId);
                    }
                }
            }

            hippoResult.setData(serializer.serialize(table));
            hippoResult.setSuccess(true);
        } catch (IOException e) {
            LOG.error("processSyncTimeRequest | IOException when serialize taskTable", e);
        } catch (Exception e) {
            LOG.error("processSyncTimeRequest | Exception bucketNo-> " + bucketNo, e);
        }

        return hippoResult;
    }

    private boolean isNeedSyncData(String bucketNo, long modifiedTime) {
        return !bucketsLatestUpTime.replace(bucketNo, modifiedTime, modifiedTime);
    }

    private boolean isNeedSyncExpired(String bucketNo, long expiredUpdatedTime) {
        return !bucketsExpiredUpdatedTime.replace(bucketNo, expiredUpdatedTime, expiredUpdatedTime);
    }

    private void whenFinishedReplicateBucket(String bucketNo, String clientUrl) {
        this.broker.whenMigrateBucketFinishedCallback(Integer.parseInt(bucketNo), clientUrl);
    }

    /* private void whenFailedReplicateBucket(String bucketNo, String clientUrl) {
        this.broker.whenMigrateBucketFinishedCallback(Integer.parseInt(bucketNo), clientUrl);
    }*/

    public HippoResult processSyncDataRequest(ReplicatedBucketDataRequest request) {
        HippoResult result = null;
        String operation = request.getHeaders().get("operation");
        if (operation.equals("block_verify")) {
            result = processVerifyExpireRequest(request);
        } else if (operation.equals("block_data")) {
            result = processReplicatedBucketData(request);
        } else {
            LOG.warn("processSyncDataRequest | no opertion defined -> " + operation + " in processSyncDataRequest!!");
        }
        result.getAttrMap().put("bucketNo", request.getBucketNo());
        result.getAttrMap().put("operation", operation);
        return result;
    }

    /**
     * verify the expire
     * @param request
     * @return
     */
    private HippoResult processVerifyExpireRequest(ReplicatedBucketDataRequest request) {
        HippoResult result = new HippoResult(false);
        String clientId = request.getHeadValue("clientId");
        String bucketNo = request.getBucketNo();

        try {
            byte[] data = request.getData();
            if (data != null) {
                List<SyncDataTask> verifyList = serializer.deserialize(data, List.class);
                if (verifyList != null && verifyList.size() > 0) {
                    List<SyncDataTask> deleteTasks = migrationEngine.verifyExpiredDbIfs(verifyList, bucketNo);
                    result.setData(serializer.serialize(deleteTasks));
                }
            }
            result.setSuccess(true);
            result.getAttrMap().put("bucketNo", bucketNo);
            result.getAttrMap().put("originalVerifySize", request.getHeadValue("originalVerifySize"));
        } catch (IOException e) {
            LOG.error("processVerifyExpireRequest | IOException when deserialize from clientId -> " + clientId, e);
        } catch (ClassNotFoundException e) {
            LOG.error("processVerifyExpireRequest | ClassNotFoundException when deserialize from clientId -> " + clientId, e);
        } catch (InstantiationException e) {
            LOG.error("processVerifyExpireRequest | InstantiationException when deserialize from clientId -> " + clientId, e);
        } catch (IllegalAccessException e) {
            LOG.error("processVerifyExpireRequest | IllegalAccessException when deserialize from clientId -> " + clientId, e);
        }

        return result;
    }

    /**
     * block data sync
     * @param request
     * @return
     * */
    private HippoResult processReplicatedBucketData(ReplicatedBucketDataRequest request) {
        HippoResult result = new HippoResult(false);
        //long beginTime = System.currentTimeMillis();
        String clientId = request.getHeadValue("clientId");
        try {
            byte[] data = migrationEngine.migration(request.getBucketNo(), request.getSizeFlag(), request.getDbinfo(), 0, MdbConstants.CAPACITY_SIZE);
            result.setData(data);
            result.setSuccess(true);
            result.getAttrMap().put("dbinfo", request.getDbinfo());
            result.getAttrMap().put("bucketNo", request.getBucketNo());
            result.getAttrMap().put("sizeFlag", request.getSizeFlag());
        } catch (Exception e) {
            LOG.error(String.format("Error when doing migration for dbinfo[%s],sizeFlag[%s],bucketNo[%s],clientId[%s]", request.getDbinfo(), request
                .getSizeFlag(), request.getBucketNo(), clientId), e);
        } finally {
            //long endTime = System.currentTimeMillis();
            /*LOG.info(String
                .format("processReplicatedBucketDataRequest cost time is %d,{bucketNo[%s],dbinfoID[%s],offset[%s],sizeFlag[%s],clientId[%s]", (endTime - beginTime), request
                    .getBucketNo(), request.getDbinfo(), request.getOffset(), request.getSizeFlag(), clientId));*/
        }
        return result;
    }

    public HippoResult processHeartBeatRequest(HeartBeatRequest request) {
        HippoResult result = new HippoResult(true);
        String clientId = request.getClientId();
        if (migrationEngine == null || !migrationEngine.isStarted()) {
            LOG.warn("processHeartBeatRequest | migrationEngine is not set, return sync success back!!");
        } else {
            Map<String, Long> syncTime = request.getSyncTime();
            Map<String, Long> expireUpdatedTime = request.getExpireUpdatedTime();

            if (syncTime != null && syncTime.size() > 0) {
                Map<String, String> newDataUpdated = new HashMap<String, String>();
                result.getAttrMap().put("bucketsRequired", syncTime.keySet().toString());
                for (String buckNo : syncTime.keySet()) {
                    Long compareTime = syncTime.get(buckNo);
                    Long expiredUpdatedTime = expireUpdatedTime.get(buckNo);
                    boolean isNeedSyncTime = isNeedSyncData(buckNo, compareTime);

                    if (expiredUpdatedTime != ReplicatedConstants.NOT_NEED_SYNC) {
                        //need to verify the expire time
                        boolean isNeedVerifyExpire = isNeedSyncExpired(buckNo, expiredUpdatedTime);

                        if (!isNeedSyncTime && !isNeedVerifyExpire) {
                            LOG.info("processHeartBeatRequest | clientId->" + clientId + " data has sync all data with the server! bucketNo -> " + buckNo);
                        } else {
                            newDataUpdated.put(buckNo, isNeedSyncTime + "," + isNeedVerifyExpire);
                        }
                    } else {
                        //the bucket do not bring the expire time
                        if (!isNeedSyncTime) {
                            LOG.info("processHeartBeatRequest | clientId->" + clientId + " data has synced the data changed , but do not need verify the expire data ! bucketNo -> " + buckNo);
                        } else {
                            newDataUpdated.put(buckNo, isNeedSyncTime + "," + false);
                        }
                    }
                }
                try {
                    result.setData(serializer.serialize(newDataUpdated));
                } catch (IOException e) {
                    LOG.error("processHeartBeatRequest | IOException in processHeartBeatRequest", e);
                }
            }
        }

        LOG.info("processHeartBeatRequest | async heart beat request from client id ->" + clientId + " has been processd!!");

        return result;
    }

    private class MdbUpdateBucketTime extends TransDataCallBack {
        @Override
        public void updateModifiedTimeCallBack(int bucket, long dbUpdatedTime) {
            while (true) {
                Long time = bucketsLatestUpTime.get(bucket + "");
                if (time != null) {
                    if (time < dbUpdatedTime) {
                        if (bucketsLatestUpTime.replace(bucket + "", time, dbUpdatedTime)) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    bucketsLatestUpTime.putIfAbsent(bucket + "", 0L);
                }
            }
        }

        @Override
        public void updateExpiredCallBack(int bucket, long dbUpdatedTime) {
            while (true) {
                Long time = bucketsExpiredUpdatedTime.get(bucket + "");
                if (time != null) {
                    if (time < dbUpdatedTime) {
                        if (bucketsExpiredUpdatedTime.replace(bucket + "", time, dbUpdatedTime)) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    LOG.warn("the bucket " + bucket + " not existed in the bucketsExpiredUpdatedTime!!");
                    break;
                }
            }
        }
    }

    @Override
    public boolean resetBuckets(List<BucketInfo> resetBuckets, boolean clearTriggerReplicatedEvent) {
        LOG.info("begin master resetBuckets : " + resetBuckets + " , existed buckets -> " + bucketsList);

        try {
            //get remove list and added list from buckets and resetBuckets
            List<String> newMasterBuckets = new ArrayList<String>();
            for (BucketInfo info : resetBuckets) {
                /// let slave open for migrate, slave will decide the right master to sync data
                /// if (!info.isSlave()) {
                newMasterBuckets.add(info.getBucketNo() + "");
                /// }

                Integer bucketNo = info.getBucketNo();
                long latestTime = migrationEngine.getBucketLatestModifiedTime(bucketNo, true);
                LOG.info("resetBuckets | init the bucket latest time for the bucket " + bucketNo + ", the time is " + latestTime);
                bucketsLatestUpTime.put(bucketNo + "", latestTime);
            }

            List<String> addedList = ListUtils.getDiffItemsFromSource(newMasterBuckets, bucketsList);
            List<String> removeList = ListUtils.getDiffItemsFromSource(bucketsList, newMasterBuckets);

            for (String bucket : removeList) {
                bucketsLatestUpTime.remove(bucket);
                bucketsExpiredUpdatedTime.remove(bucket);
            }

            for (String bucket : addedList) {
                bucketsLatestUpTime.putIfAbsent(bucket, 0L);
                bucketsExpiredUpdatedTime.putIfAbsent(bucket, 0L);
            }

            bucketsList.clear();
            bucketsList.addAll(newMasterBuckets);

            if (migrationEngine.isStarted()) {
                migrationEngine.resetBuckets(resetBuckets);
            }

            if (clearTriggerReplicatedEvent) {
                // DPJ: in order to prevent: mt change -> clear trigger -> heart beat call when -> mt change 
                // TODO: bug: if A up, B up, B down (quick table failed), B up. triggerReplicatedEvent won't be clear in A, and whenFinishedReplicateBucket won't be called
                LOG.info("triggerReplicatedEvent clear trigger!!");
                triggerReplicatedEvent.clear();
            }

            if (migrationEngine.getDataTransCallBackList() == null || migrationEngine.getDataTransCallBackList().size() == 0) {
                List<TransDataCallBack> callbacks = new ArrayList<TransDataCallBack>();
                callbacks.add(new MdbUpdateBucketTime());
                migrationEngine.setDataTransCallBackList(callbacks);
            }

            migrationEngine.startMdbStoreEnginePeriodsExpire();
            return true;
        } catch (Exception e) {
            LOG.error("error when doing reset the master buckets!", e);
            return false;
        } finally {
            LOG.info("after master resetBuckets : " + bucketsList);
        }
    }

    public void setMigerateEngine(MigrationEngine migrationEngine) {
        if (migrationEngine instanceof MdbMigrationEngine) {
            this.migrationEngine = (MdbMigrationEngine) migrationEngine;
        }
    }
}
