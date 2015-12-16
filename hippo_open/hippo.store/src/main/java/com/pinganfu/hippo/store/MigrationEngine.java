package com.pinganfu.hippo.store;

import java.util.List;
import java.util.Map;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 * 2015-1-12
 */
public interface MigrationEngine extends LifeCycle {

    /**
     * become Master Bucket need interface
     * @param bucketNo
     * @return
     */
    public Map<String, List<String>> getBucketStorageInfo(String bucketNo);

    /**
     * @param bucketNo
     * @param blockNo
     * @param offset
     * @param size
     * @return
     */
    public byte[] migration(String bucketNo, String sizetype, String blockNo, int offset, int size);

    /**
     * @param bucketNo
     * @param sizeType
     * @param blockNo
     * @param data
     * @return
     * @throws HippoStoreException
     */
    public boolean replicated(String bucketNo, String sizeType, String blockNo, byte[] data) throws HippoStoreException;

    /**
     * reset the buckets
     * @param bucketNos
     */
    public void resetBuckets(List<BucketInfo> bucketNos);
    
    public String getName();

    //-----------------------------level db -------------------------

    /**
     * @param standby
     * @param bucket
     * @param offset
     * @param size
     * @return
     */
    public byte[] migration(String standby, int bucket, Object offset, int size);
}
