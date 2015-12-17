package com.hippo.broker.cluster.simple.master.mdb;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.HeartBeatRequest;
import com.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.hippo.broker.cluster.server.MasterReplicatedCommandManager;
import com.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.hippo.client.HippoResult;
import com.hippo.common.SyncDataTask;
import com.hippo.common.SyncTaskTable;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.exception.HippoException;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbMigrationEngine;
import com.hippo.network.ServerFortress;
import com.hippo.network.ServerFortressFactory;
import com.hippo.network.impl.TransportServerFortressFactory;
import com.hippo.network.transport.nio.server.NioTransportConnectionManager;
import com.hippo.store.MigrationEngine;
import com.hippo.store.TransDataCallBack;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author owen
 */
public class MdbMasterReplicatedServer extends IMasterReplicatedServer {
    protected static final Logger LOG = LoggerFactory.getLogger(MdbMasterReplicatedServer.class);

    private Serializer serializer = null;

    private NioTransportConnectionManager connectionManager;

    private ServerFortress serverFortress;

    private MdbMigrationEngine migrationEngine;

    private int replicated_port = 61100;

    private ConcurrentHashMap<String, Long> bucketsLastestUpTime = new ConcurrentHashMap<String, Long>();

    private ConcurrentHashMap<String, Long> bucketsExpiredUpdatedTime = new ConcurrentHashMap<String, Long>();

    public MdbMasterReplicatedServer(MigrationEngine migrationEngine, String replicated_port) {
        if (migrationEngine instanceof MdbMigrationEngine) {
            this.migrationEngine = (MdbMigrationEngine) migrationEngine;
        }

        if (StringUtils.isNotEmpty(replicated_port)) {
            this.replicated_port = Integer.parseInt(replicated_port);
        }
    }

    @Override
    public void doInit() {
        LOG.info("MasterReplicatedServer begin to do init");

        migrationEngine.init();

        List<TransDataCallBack> callbacks = new ArrayList<TransDataCallBack>();
        callbacks.add(new MdbUpdateBucketTime());
        migrationEngine.setDataTransCallBackList(callbacks);

        LOG.info("AsyncCallback was set successfully");

        if (serializer == null) {
            serializer = new KryoSerializer();
        }

        connectionManager = new NioTransportConnectionManager();

        ServerFortressFactory sfactory = new TransportServerFortressFactory(TransportServerFortressFactory.DEFAULT_SHEME, replicated_port, new MasterReplicatedCommandManager(this), connectionManager);
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

            migrationEngine.startMdbStoreEnginePeriodsExpire();

            List<BucketInfo> allBuckets = migrationEngine.getBuckets();

            for (BucketInfo bucket : allBuckets) {
                if (!bucket.isSlave()) {
                    Integer bucketNo = bucket.getBucketNo();
                    long latestTime = migrationEngine.getBucketLatestModifiedTime(bucketNo, false);
                    LOG.info("init the bucket latest time for the bucket " + bucketNo + ", the time is " + latestTime);
                    bucketsExpiredUpdatedTime.put(bucketNo + "", 0L);
                    bucketsLastestUpTime.put(bucketNo + "", latestTime);
                }
            }

            LOG.info("MasterReplicatedServer finished doing doStart");
        } catch (Exception e) {
            LOG.error("MdbMasterReplicatedServer started failed!!", e);
        }
    }

    @Override
    public void doStop() {
        LOG.info("MasterReplicatedServer begin to do doStop");

        if (serverFortress != null) {
            serverFortress.stop();
        }

        migrationEngine.stopMdbStoreEnginePeriodsExpire();

        LOG.info("MdbStoreEngine stop the periods expire thread successfully");
    }

    /**
     * process the data sync list according the bucket number and time
     *
     * @param request
     * @return
     */
    public HippoResult processSyncTimeRequest(ReplicatedBucketRequest request) {
        HippoResult hippoResult = new HippoResult(false);
        String bucketNo = request.getBuckNo();
        hippoResult.putAttribute("bucketNo", bucketNo);

        long modifiedTime = request.getModifiedTime();
        long expiredUpdateTime = request.getExpiredUpdateTime();

        boolean isNeedSyncExpire = false;
        if (expiredUpdateTime != ReplicatedConstants.NOT_NEED_SYNC) {
            isNeedSyncExpire = isNeedSyncExpired(bucketNo, expiredUpdateTime);
        }

        boolean isNeedSync = false;
        if (modifiedTime != ReplicatedConstants.NOT_NEED_SYNC) {
            isNeedSync = isNeedSyncData(bucketNo, modifiedTime);
        }

        SyncTaskTable table = new SyncTaskTable();
        if (isNeedSync) {
            long endTime = bucketsLastestUpTime.get(bucketNo);
            Map<String, Set<String>> result = migrationEngine.collectLatestDbInfoIds(bucketNo, modifiedTime, endTime);
            table.setDbInfos(result);
            table.setEndSyncTime(endTime);
        } else {
            table.setEndSyncTime(ReplicatedConstants.NOT_NEED_SYNC);
        }

        if (isNeedSyncExpire) {
            long endTime = bucketsExpiredUpdatedTime.get(bucketNo);
            table.setEndExpiredTime(endTime);
        } else {
            table.setEndExpiredTime(ReplicatedConstants.NOT_NEED_SYNC);
        }

        try {
            hippoResult.setData(serializer.serialize(table));
            hippoResult.setSuccess(true);
        } catch (IOException e) {
            LOG.error("processSyncTimeRequest | IOException when serialize taskTable in processSyncTimeRequest", e);
        }

        return hippoResult;
    }

    private boolean isNeedSyncData(String bucketNo, long modifiedTime) {
        return !bucketsLastestUpTime.replace(bucketNo, modifiedTime, modifiedTime);
    }

    private boolean isNeedSyncExpired(String bucketNo, long expiredUpdatedTime) {
        return !bucketsExpiredUpdatedTime.replace(bucketNo, expiredUpdatedTime, expiredUpdatedTime);
    }

    public HippoResult processSyncDataRequest(ReplicatedBucketDataRequest request) {
        HippoResult result = null;
        String operation = request.getHeaders().get("operation");
        if (operation.equals("block_verify")) {
            result = processVerifyExpireRquest(request);
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
     *
     * @param request
     * @return
     */
    private HippoResult processVerifyExpireRquest(ReplicatedBucketDataRequest request) {
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
            LOG.error("processVerifyExpireRquest | IOException when deserialize from clientId -> " + clientId, e);
        } catch (ClassNotFoundException e) {
            LOG.error("processVerifyExpireRquest | ClassNotFoundException when deserialize from clientId -> " + clientId, e);
        } catch (InstantiationException e) {
            LOG.error("processVerifyExpireRquest | InstantiationException when deserialize from clientId -> " + clientId, e);
        } catch (IllegalAccessException e) {
            LOG.error("processVerifyExpireRquest | IllegalAccessException when deserialize from clientId -> " + clientId, e);
        }

        return result;
    }

    /**
     * block data sync
     *
     * @param request
     * @return
     */
    private HippoResult processReplicatedBucketData(ReplicatedBucketDataRequest request) {
        HippoResult result = new HippoResult(false);
        long beginTime = System.currentTimeMillis();
        String clientId = request.getHeadValue("clientId");
        try {
            byte[] data = migrationEngine.migration(request.getBucketNo(), request.getSizeFlag(), request.getDbinfo(), 0, MdbConstants.CAPACITY_SIZE);
            result.setData(data);
            result.setSuccess(true);
            result.getAttrMap().put("dbinfo", request.getDbinfo());
            result.getAttrMap().put("bucketNo", request.getBucketNo());
            result.getAttrMap().put("sizeFlag", request.getSizeFlag());
        } catch (Exception e) {
            LOG.error(String.format("processReplicatedBucketData | Error when doing migration for dbinfo[%s],sizeFlag[%s],bucketNo[%s],clientId[%s]", request.getDbinfo(), request
                    .getSizeFlag(), request.getBucketNo(), clientId), e);
        } finally {
            long endTime = System.currentTimeMillis();
            LOG.info(String
                    .format("processReplicatedBucketDataRequest cost time is %d,{bucketNo[%s],dbinfoID[%s],offset[%s],sizeFlag[%s],clientId[%s]", (endTime - beginTime), request
                            .getBucketNo(), request.getDbinfo(), request.getOffset(), request.getSizeFlag(), clientId));
        }
        return result;
    }

    /**
     * process the heart beat from the client
     *
     * @param request
     * @return
     */
    public HippoResult processHeartBeatRequest(HeartBeatRequest request) {
        HippoResult result = new HippoResult(true);

        String clientId = request.getClientId();

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
                LOG.error("IOException in processHeartBeatRequest", e);
            }
        }

        LOG.info("processHeartBeatRequest | async heart beat request from client id ->" + clientId + " has been processd!!");

        return result;
    }

    //for test
    public HippoResult processVerification(ReplicatedBucketRequest command) {
        HippoResult hippoResult = new HippoResult(true);
        String bucketNo = command.getBuckNo();
        Map<String, List<String>> map = migrationEngine.getBucketStorageInfo(bucketNo);
        try {
            hippoResult.setData(serializer.serialize(map));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hippoResult;
    }

    @Override
    public void setMigerateEngine(MigrationEngine migrationEngine) {
    }

    private class MdbUpdateBucketTime extends TransDataCallBack {
        @Override
        public void updateModifiedTimeCallBack(int bucket, long dbUpdatedTime) {
            while (true) {
                Long time = bucketsLastestUpTime.get(bucket + "");
                if (time != null) {
                    if (time < dbUpdatedTime) {
                        if (bucketsLastestUpTime.replace(bucket + "", time, dbUpdatedTime)) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    bucketsLastestUpTime.putIfAbsent(bucket + "", 0L);
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
                    LOG.warn("updateExpiredCallBack | the bucket " + bucket + " not existed in the bucketsExpiredUpdatedTime!!");
                    break;
                }
            }
        }
    }
}
