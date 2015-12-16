package com.pinganfu.hippo.mdb;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pinganfu.hippo.common.SyncDataTask;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.mdb.exception.OutOfMaxCapacityException;
import com.pinganfu.hippo.mdb.obj.DBInfo;
import com.pinganfu.hippo.mdb.obj.MdbPointer;
import com.pinganfu.hippo.store.TransDataCallBack;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/***
 * 
 * @author saitxuc
 * write 2014-7-25
 */
public interface MdbManager extends LifeCycle {

    /**
     * @param key
     * @param value
     * @param expire
     * @param operAction
     * @return
     */
    public MdbResult offerOper(byte[] key, byte[] value, int expire, Integer bucketNo, int version, String operAction) throws HippoStoreException;

    /**
     * @param capacity
     * @param blockSize
     * @return
     * @throws OutOfMaxCapacityException
     */
    public DBInfo createDirectBufferInfo(int capacity, String blockSize, String dbNo, Integer bucketNo) throws OutOfMaxCapacityException;

    /**
     * @return
     */
    public int countDB();

    /**
     * @param bucketNo
     * @param sizeFlag
     * @param dbIfId
     * @param offset
     * @param size
     * @return
     */
    public byte[] duplicateDirectBuffer(Integer bucketNo, String sizeFlag, String dbIfId, int offset, int size);

    /**
     * 
     * @return
     * @throws IOException
     */
    public byte[] getSeparator() throws IOException;

    /**
     * 
     * @param sizeKey
     * @return
     */
    public DBAssembleManager getDBAssembleManager(String sizeKey);

    /**
     * @return
     */
    public int getSize();

    /**
     * @return
     */
    public long getCurrentUsedCapacity();

    /**
     * @param key
     * @param dbIfo
     * @param offset
     */
    public void deleteByKey(byte[] key, DBInfo dbIfo, int offset);

    /**
     * @param capacity
     * @param bucketNo
     */
    public void reduceCurrentCapacity(int capacity, Integer bucketNo);

    /**
     * @param bucketNo
     * @return
     */
    public Map<String, List<String>> collectDBIfs(Integer bucketNo);

    /***
     * 
     * @param resetBuckets
     */
    public void resetBuckets(List<BucketInfo> resetBuckets);

    /**
     * @param parseInt
     * @param sizeFlag
     * @param dbIfId
     */
    public void deleteDBIf(int parseInt, String sizeFlag, String dbIfId);

    /**
     */
    public void doExpireAndGetDeletedDBIfs();

    /**
     * @return
     */
    public List<TransDataCallBack> getDataTransCallBackList();

    /**
     * @param callbacks
     */
    public void setDataTransCallBackList(List<TransDataCallBack> callbacks);

    /**
     * @param bucketNo
     * @param sizeFlag
     * @param dbInfoId
     * @param offset
     * @param data
     * @return
     * @throws HippoStoreException
     */
    public boolean syncDBIf(Integer bucketNo, String sizeFlag, String dbInfoId, int offset, byte[] data) throws HippoStoreException;

    /**
     * @param bucketNo
     * @param beginTime
     * @param endTime
     * @return
     */
    public Map<String, Set<String>> collectDBIfsBetweenGivenTime(Integer bucketNo, long beginTime, long endTime);

    /**
     * @param capacitySize
     * @param sizeModel
     * @param createDbId
     * @param bucketNo
     * @return
     * @throws OutOfMaxCapacityException 
     */
    public DBInfo fetchNewDBInfo(int capacitySize, String sizeModel, String createDbId, Integer bucketNo) throws OutOfMaxCapacityException;

    /**
     * @param bucketNo
     * @param modifyTime
     */
    public void expireOffsetCallBack(Integer bucketNo, long modifyTime);

    /**
     * @param bucketNo
     * @param useDefault 
     * @return
     */
    public long getBucketLatestModifiedTime(Integer bucketNo, boolean useDefault);

    /**
     * @param dbIf
     * @param offset
     */
    public void expireOffsetTime(DBInfo dbIf, int offset);

    /**
     * @param key
     * @param dbIf
     * @param offset
     */
    public void removeKeyFromMap(byte[] key, DBInfo dbIf, int offset);

    /**
     * @param bucketNo
     * @return
     */
    public long getBucketUsedCapacity(Integer bucketNo);

    /**
     * @param length
     * @return
     */
    public String getSizeType(int length);

    /**
     * @param dbIfs
     * @param bucketNo
     * @param sizeFlag 
     * @throws HippoStoreException 
     */
    public List<SyncDataTask> getTasksNotExistedInSyncList(Set<String> dbIfs, Integer bucketNo, String sizeFlag) throws HippoStoreException;

    /**
     * @param bucketNo
     * @param sizeFlag
     * @param dbIfId
     * @return
     */
    public boolean verifyExpiredDbInfo(Integer bucketNo, String sizeFlag, String dbIfId);

    /**
     * @param bucketNo
     */
    public LinkedList<SyncDataTask> emergencyVerifyBucket(Integer bucketNo) throws HippoStoreException;

    /**
     * @param bucketNo
     * @return
     */
    public double getUsedPercent(Integer bucketNo) throws HippoStoreException;

    /**
     * @param bucketNo
     * @return
     * @throws HippoStoreException 
     */
    public List<SyncDataTask> getWholeDBIfs(Integer bucketNo) throws HippoStoreException;

    /**
     * @param point
     * @param key
     */
    void expireOffsetTimeWhenSyncData(MdbPointer point, byte[] key) throws HippoStoreException;
    
    /**
     * @param bucketNo
     * @return
     */
    public boolean memoryCheckFull(Integer bucketNo);
    
    /**
     * @param key
     * @param offset
     * @param expire
     * @param bucketNo
     * @param version
     * @param bitsetOper
     * @param val
     * @return
     */
    MdbResult offerBitOper(byte[] key, int offset, int expire, int bucketNo, int version, String bitsetOper, boolean val) throws HippoStoreException;
}
