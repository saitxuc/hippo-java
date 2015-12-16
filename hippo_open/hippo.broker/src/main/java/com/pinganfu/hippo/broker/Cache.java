package com.pinganfu.hippo.broker;

import com.pinganfu.hippo.client.HippoResult;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.store.StoreEngine;

import java.util.List;
import java.util.Map;

/**
 * @author saitxuc
 *         write 2014-7-22
 */
public interface Cache extends LifeCycle {

    /**
     * @param expire
     * @param key
     * @param value
     * @return
     */
    public HippoResult set(int expire, byte[] key, byte[] value, int bucketNo);

    /**
     * @param expire
     * @param key
     * @param value
     * @param version
     * @return
     */
    public HippoResult set(int expire, byte[] key, byte[] value, int version, int bucketNo);

    /**
     * @param expire
     * @param key
     * @param value
     * @param version
     * @return
     */
    public HippoResult update(int expire, byte[] key, byte[] value, int version, int bucketNo);

    /**
     * @param key
     * @return
     */
    public HippoResult get(byte[] key, int bucketNo);

    /**
     * @param key
     * @param version
     * @return
     */
    public HippoResult get(byte[] key, int version, int bucketNo);

    /**
     * @param key
     * @return
     */
    public HippoResult remove(byte[] key, int bucketNo);

    /**
     * @param key
     * @param version
     * @return
     */
    public HippoResult remove(byte[] key, int version, int bucketNo);

    public HippoResult getBit(byte[] key, int offset, int bucketNo);

    public HippoResult setBit(int expire, byte[] key, int offset, boolean val, int bucketNo);

    public HippoResult updateBit(int expire, byte[] key, int offset, boolean val, int bucketNo);

    public HippoResult removeBit(byte[] key, int bucketNo);

    /**
     * @return
     */
    public StoreEngine getEngine();

    /**
     *
     */
    public void release();

    /**
     * @param brokerService
     */
    void setBrokerService(BrokerService brokerService);

    /**
     *
     */
    void checkStoreEngineConfig();

    /**
     * @param limit
     */
    void setLimit(long limit);

    /**
     * @param buckets
     */
    void setBuckets(List<BucketInfo> buckets);

    /**
     * @param bucketLimit
     */
    void setBucketLimit(int bucketLimit);

    /**
     * @param params
     */
    void setInitParams(Map<String, String> params);
}
