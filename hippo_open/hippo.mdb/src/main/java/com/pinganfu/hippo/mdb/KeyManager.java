package com.pinganfu.hippo.mdb;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.mdb.obj.MdbPointer;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author saitxuc
 *         write 2014-7-28
 */
public interface KeyManager {

    /**
     * @param key
     * @param info
     */
    public void setKeyStoreInfo(byte[] key, MdbPointer info);

    /**
     * @param key
     * @return
     */
    public MdbPointer getStoreInfo(byte[] key);

    /**
     * @param key
     * @return
     */
    public MdbPointer removeStoreInfo(byte[] key);

    /**
     * @param key
     * @param newInfo
     * @return
     */
    public MdbPointer replaceStoreInfo(byte[] key, MdbPointer newInfo);

    /**
     * @param key
     * @param info
     * @param bucketNo
     */
    public void setKeyStoreInfo(byte[] key, MdbPointer info, Integer bucketNo);

    /**
     * @param key
     * @param bucketNo
     * @return
     */
    public MdbPointer getStoreInfo(byte[] key, Integer bucketNo);

    /**
     * @param key
     * @param bucketNo
     * @return
     */
    public MdbPointer removeStoreInfo(byte[] key, Integer bucketNo);

    /**
     * @param key
     * @param newInfo
     * @param bucketNo
     * @return
     */
    public MdbPointer replaceStoreInfo(byte[] key, MdbPointer newInfo, Integer bucketNo);

    /**
     * @return
     */
    public int getSize();

    /**
     * @param bucketNo
     * @return
     */
    public int getSize(Integer bucketNo);

    /**
     * @param buckets
     */
    public void resetBuckets(List<BucketInfo> buckets);

    /**
     * @param key
     * @param bucketNo
     * @return
     */
    public boolean contain(byte[] key, Integer bucketNo);

    /**
     * @param bucketNo
     * @return
     */
    public Set<Entry<byte[], MdbPointer>> getEntrySet(Integer bucketNo);
}
