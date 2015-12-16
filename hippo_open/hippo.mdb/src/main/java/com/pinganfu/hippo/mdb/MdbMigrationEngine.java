package com.pinganfu.hippo.mdb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.common.SyncDataTask;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.store.MigrationEngine;
import com.pinganfu.hippo.store.StoreEngine;
import com.pinganfu.hippo.store.TransDataCallBack;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author Owen
 * @version $Id: MdbMigrationEngine.java, v 0.1 2015年3月23日 下午10:30:19 Owen Exp $
 */
public class MdbMigrationEngine extends LifeCycleSupport implements MigrationEngine {
    protected static final Logger LOG = LoggerFactory.getLogger(MdbMigrationEngine.class);

    private MdbManager mdbManager = null;

    private StoreEngine storeEngine;

    public MdbMigrationEngine(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }

    /**
     * Map<String, List<String>> 
     * 
     * String 为sizeFlag
     * 
     * List<String> 为对应sizeflag的dbinfo id
     * */
    @Override
    public Map<String, List<String>> getBucketStorageInfo(String bucketNo) {
        return mdbManager.collectDBIfs(Integer.parseInt(bucketNo));
    }

    /**
     * send all the data to other servers
     * @param bucketNo 
     * @param blockNo dbIf's id
     * @param offset offset in the dbIf
     * @param size size_string
     * */
    @Override
    public byte[] migration(String bucketNo, String sizetype, String blockNo, int offset, int size) {
        int buckNum = Integer.parseInt(bucketNo);
        return mdbManager.duplicateDirectBuffer(buckNum, sizetype, blockNo, offset, size);
    }

    @Override
    public boolean replicated(String bucketNo, String sizeType, String blockNo, byte[] data) throws HippoStoreException {
        int buckNum = Integer.parseInt(bucketNo);
        return mdbManager.syncDBIf(buckNum, sizeType, blockNo, 0, data);
    }

    @Override
    public boolean isStarted() {
        return mdbManager != null && mdbManager.isStarted();
    }

    @Override
    public void doInit() {
        LOG.info("MdbMigrationEngine begin to do init!!");

        if (storeEngine == null) {
            throw new UnsupportedOperationException("store engine was not set!!");
        }

        if (storeEngine instanceof MdbStoreEngine) {
            LOG.info("detect the MdbStoreEngine in MdbMigrationEngine init!!");
        } else {
            throw new UnsupportedOperationException("store engine was set wrong in MdbMigrationEngine!!");
        }

        MdbStoreEngine mdbStoreEngine = (MdbStoreEngine) storeEngine;

        if (mdbStoreEngine.getMdbManager() == null) {
            throw new UnsupportedOperationException("MdbManager is null in mdbStoreEngine when do MdbMigrationEngine init");
        }

        if (mdbStoreEngine.getMdbManager() instanceof MdbManager) {
            mdbManager = (MdbManager) mdbStoreEngine.getMdbManager();
            LOG.info("MdbMigrationEngine init successfully!!");
        } else {
            throw new UnsupportedOperationException("MdbManager is not right MdbManagerImpl in mdbStoreEngine when do MdbMigrationEngine init");
        }
    }

    @Override
    public void doStart() {
        //mdbManager.start();
        //storeEngine.start();
    }

    @Override
    public void doStop() {
        //mdbManager.stop();
        //storeEngine.stop();
    }

    public Map<String, Set<String>> collectLatestDbInfoIds(String buckId, long beginTime, long endTime) {
        return mdbManager.collectDBIfsBetweenGivenTime(Integer.parseInt(buckId), beginTime, endTime);
    }

    public void deleteDBIf(String buckId, String sizeFlag, String dbIf) {
        mdbManager.deleteDBIf(Integer.parseInt(buckId), sizeFlag, dbIf);
    }

    @Override
    public void resetBuckets(List<BucketInfo> bucketNos) {
        mdbManager.resetBuckets(bucketNos);
    }

    public List<BucketInfo> getBuckets() {
        return storeEngine.getBuckets();
    }

    public void setDataTransCallBackList(List<TransDataCallBack> callbacks) {
        ((MdbStoreEngine) storeEngine).setDataTransCallBackList(callbacks);
    }

    public List<TransDataCallBack> getDataTransCallBackList() {
        return ((MdbStoreEngine) storeEngine).getDataTransCallBackList();
    }

    public void startMdbStoreEnginePeriodsExpire() {
        ((MdbStoreEngine) storeEngine).startPeriodsExpire();
    }

    public void stopMdbStoreEnginePeriodsExpire() {
        ((MdbStoreEngine) storeEngine).stopPeriodsExpire();
    }

    public long getBucketLatestModifiedTime(int bucketNo, boolean useDefault) {
        return ((MdbStoreEngine) storeEngine).getBucketLatestModifiedTime(bucketNo, useDefault);
    }

    @Override
    public String getName() {
        return storeEngine.getName();
    }

    public LinkedList<SyncDataTask> getTasksNotExistedInSyncList(Map<String, Set<String>> dataMap, String bucketNo) throws HippoStoreException {
        LinkedList<SyncDataTask> result = new LinkedList<SyncDataTask>();
        if (dataMap != null) {
            for (Entry<String, Set<String>> entry : dataMap.entrySet()) {
                String sizeFlag = entry.getKey();
                Set<String> dbIfs = entry.getValue();
                if (dbIfs != null && dbIfs.size() > 0) {
                    List<SyncDataTask> subResult = mdbManager.getTasksNotExistedInSyncList(dbIfs, Integer.parseInt(bucketNo), sizeFlag);
                    result.addAll(subResult);
                    subResult.clear();
                }
            }
        } else {
            List<SyncDataTask> subResult = mdbManager.getWholeDBIfs(Integer.parseInt(bucketNo));
            result.addAll(subResult);
            subResult.clear();
        }
        return result;
    }

    /**
     * verify the list in the store engine and return the list which needs to be deleted in the client 
     * @param verifyList
     * @param bucketNo
     * @return
     */
    public List<SyncDataTask> verifyExpiredDbIfs(List<SyncDataTask> verifyList, String bucketNo) {
        List<SyncDataTask> deletedList = new ArrayList<SyncDataTask>();
        for (SyncDataTask task : verifyList) {
            boolean isContain = mdbManager.verifyExpiredDbInfo(Integer.parseInt(bucketNo), task.getSizeFlag(), task.getDbinfoId());
            if (!isContain) {
                deletedList.add(task);
            }
        }
        return deletedList;
    }

    /**
     * when happen out of memory , need to do emergency verify
     * @param bucketNo
     * @return 
     */
    public LinkedList<SyncDataTask> emergencyVerifyBucket(String bucketNo) throws HippoStoreException {
        return mdbManager.emergencyVerifyBucket(Integer.parseInt(bucketNo));
    }

    /**
     * when happen out of memory , need to do emergency verify
     * @param bucketNo
     * @return 
     */
    public double getUsedPercent(String bucketNo) throws HippoStoreException {
        return mdbManager.getUsedPercent(Integer.parseInt(bucketNo));
    }

    @Override
    public byte[] migration(String standby, int bucket, Object offset, int size) {
        throw new UnsupportedOperationException();
    }

}
