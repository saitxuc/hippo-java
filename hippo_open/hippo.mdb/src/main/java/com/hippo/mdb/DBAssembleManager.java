package com.hippo.mdb;

import com.hippo.common.SyncDataTask;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.mdb.obj.DBInfo;
import com.hippo.mdb.obj.MdbOperResult;
import com.hippo.mdb.obj.MdbPointer;
import com.hippo.store.exception.HippoStoreException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author saitxuc
 *         write 2014-7-29
 */
public interface DBAssembleManager extends LifeCycle {

    /**
     * @param oper
     * @return
     */
    public MdbOperResult handleOper(MdbOper oper);

    /**
     * @return
     */
    public int countDB();

    /**
     * @return
     */
    public Map<Integer, List<String>> expire();

    /**
     * @param bucketNo
     * @param dbIfId
     * @param offset
     * @param size
     * @return
     */
    public byte[] duplicateDirectBuffer(Integer bucketNo, String dbIfId, int offset, int size);

    /**
     * @return
     */
    public int getSize();

    /**
     * @param bucketNo
     * @return
     */
    public List<String> getDBIfIdsByBucketNo(Integer bucketNo);

    /**
     * @param bucketNo
     * @param dbIfId
     * @param offset
     * @param data
     * @return
     */
    public boolean syncDBIf(Integer bucketNo, String dbIfId, int offset, byte[] data) throws HippoStoreException;

    /**
     * @param bucketIfs
     */
    public void resetBuckets(List<BucketInfo> bucketIfs);

    /**
     * @param bucketNo
     * @param dbIfId
     */
    public void deleteDBIf(Integer bucketNo, String dbIfId);

    /**
     * @param bucketNo
     * @param beginTime
     * @param endTime
     * @return
     */
    public Set<String> collectDBIfsBetweenGivenTime(Integer bucketNo, long beginTime, long endTime);

    /**
     * @param bucketNo
     * @return
     */
    public long getBucketLatestModifiedTime(Integer bucketNo);

    /**
     * @param oldPoint
     */
    public void setRecyclePoint(MdbPointer oldPoint);

    /**
     * @param info
     * @param offset
     * @param modifyBuffer
     */
    public void expireOffset(DBInfo info, int offset, boolean modifyBuffer);

    /**
     * @param dbIfs
     * @param bucketNo
     * @return
     * @throws HippoStoreException
     */
    public List<SyncDataTask> getTasksNotInSyncList(Set<String> dbIfs, Integer bucketNo) throws HippoStoreException;

    /**
     * @param dbIfId
     * @param bucketNo
     * @return
     */
    public boolean verifyExpiredDbIf(String dbIfId, Integer bucketNo);

    /**
     * verify the key is correct
     * @param key
     * @param pointer
     * @return
     */
    public boolean verifyKey(byte[] key, MdbPointer pointer);
}
