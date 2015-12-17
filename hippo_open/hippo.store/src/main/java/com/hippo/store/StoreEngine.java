package com.hippo.store;

import com.hippo.common.domain.BucketInfo;
import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.store.exception.HippoStoreException;
import com.hippo.store.model.GetResult;

import java.util.List;
import java.util.Map;

/**
 * @author saitxuc
 *         write 2014-7-22
 */
public interface StoreEngine extends LifeCycle {

    /**
     * @param key
     * @param value
     * @param expire
     * @param bucketNo
     * @return
     * @throws HippoStoreException
     */
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo) throws HippoStoreException;

    /**
     * @param key
     * @param value
     * @param expire
     * @param bucketNo
     * @param version
     * @return
     * @throws HippoStoreException
     */
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo, int version) throws HippoStoreException;

    /**
     * @param key
     * @param value
     * @param expire
     * @return
     * @throws HippoStoreException
     */
    public boolean addData(byte[] key, byte[] value, int expire) throws HippoStoreException;

    /**
     * @param key
     * @return
     * @throws HippoStoreException
     */
    public GetResult getData(byte[] key) throws HippoStoreException;

    /**
     * @param key
     * @param bucketNo
     * @return
     * @throws HippoStoreException
     */
    public GetResult getData(byte[] key, int bucketNo) throws HippoStoreException;

    /**
     * @param key
     * @return
     * @throws HippoStoreException
     */
    public boolean removeData(byte[] key) throws HippoStoreException;

    /**
     * @param key
     * @param bucketNo
     * @throws HippoStoreException
     */
    public boolean removeData(byte[] key, int bucketNo) throws HippoStoreException;

    /**
     * @param key
     * @param bucketNo
     * @param version
     * @return
     * @throws HippoStoreException
     */
    public boolean removeData(byte[] key, int bucketNo, int version) throws HippoStoreException;

    /**
     * @param key
     * @param value
     * @param expire
     * @param bucketNo
     * @throws HippoStoreException
     */
    public boolean updateData(byte[] key, byte[] value, int expire, int bucketNo) throws HippoStoreException;

    /**
     * @param key
     * @param value
     * @param expire
     * @param bucketNo
     * @param version
     * @return
     * @throws HippoStoreException
     */
    public boolean updateData(byte[] key, byte[] value, int expire, int bucketNo, int version) throws HippoStoreException;

    /**
     * @param key
     * @param value
     * @param expire
     * @return
     * @throws HippoStoreException
     */
    public boolean updateData(byte[] key, byte[] value, int expire) throws HippoStoreException;

    /**
     * @return
     * @throws HippoStoreException
     */
    int getDataCount() throws HippoStoreException;

    /**
     * @return
     */
    String getName();

    /**
     * @return
     */
    long size();

    /**
     * @return
     */
    long getCurrentUsedCapacity();

    /**
     * @param params
     */
    void setExtraInitParams(Map<String, String> params);

    /**
     * @param limit
     */
    void setLimit(long limit);

    /**
     * @param buckets
     */
    void setBuckets(List<BucketInfo> buckets);

    /**
     * @return
     */
    public List<BucketInfo> getBuckets();


    /**
     * @param bucket
     * @return
     */
    long getBucketUsedCapacity(int bucket);

    /**
     * @param key
     * @param offset
     * @param val
     * @param bucketNo
     * @param expire
     * @return
     * @throws HippoStoreException
     */
    boolean setBit(byte[] key, int offset, boolean val, int bucketNo,int expire) throws HippoStoreException;

    /**
     * @param key
     * @param offset
     * @param bucketNo
     * @return GetResult
     * @throws HippoStoreException
     */
    GetResult getBit(byte[] key, int offset, int bucketNo) throws HippoStoreException;


    /**
     * @param key
     * @param bucketNo
     * @return
     * @throws HippoStoreException
     */
    boolean removeBit(byte[] key, int bucketNo) throws HippoStoreException;
}
