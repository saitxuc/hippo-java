package com.hippo.mdb.impl;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hippo.common.domain.BucketInfo;
import com.hippo.mdb.KeyManager;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbManager;
import com.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 *
 */
public class OffHeapKeyManager implements KeyManager {
    private static final Logger LOG = LoggerFactory.getLogger(OffHeapKeyManager.class);

    private final ConcurrentHashMap<Integer, OffHeapMap<byte[], MdbPointer>> bucketsHeapMap = new ConcurrentHashMap<Integer, OffHeapMap<byte[], MdbPointer>>();

    private MdbManager mdbManager;

    public OffHeapKeyManager(MdbManager mdbManager, List<BucketInfo> buckets) {
        this.mdbManager = mdbManager;
        for (BucketInfo info : buckets) {
            OffHeapMap<byte[], MdbPointer> offHeapMap = new OffHeapMap<byte[], MdbPointer>(this.mdbManager);
            bucketsHeapMap.putIfAbsent(info.getBucketNo(), offHeapMap);
        }
    }

    @Override
    public void setKeyStoreInfo(byte[] key, MdbPointer info) {
        this.setKeyStoreInfo(key, info, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public MdbPointer getStoreInfo(byte[] key) {
        return this.getStoreInfo(key, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public MdbPointer removeStoreInfo(byte[] key) {
        return this.removeStoreInfo(key, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public MdbPointer replaceStoreInfo(byte[] key, MdbPointer newInfo) {
        return this.replaceStoreInfo(key, newInfo, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public int getSize() {
        int size = 0;
        synchronized (bucketsHeapMap) {
            for (Entry<Integer, OffHeapMap<byte[], MdbPointer>> entry : bucketsHeapMap.entrySet()) {
                size += entry.getValue().size();
                //System.out.println(entry.getKey() + "-----" + entry.getValue().size());
            }
        }
        return size;
    }

    @Override
    public void setKeyStoreInfo(byte[] key, MdbPointer info, Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            map.put(key, info);
        } else {
            LOG.warn("OffHeapKeyManager | setKeyStoreInfo bucketNo -> " + bucketNo + " not existed!!");
        }
    }

    @Override
    public MdbPointer getStoreInfo(byte[] key, Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.get(key);
        } else {
            LOG.info("OffHeapKeyManager | getStoreInfo |  bucket not existed  -> " + bucketNo);
            return null;
        }

    }

    @Override
    public MdbPointer removeStoreInfo(byte[] key, Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.remove(key);
        } else {
            LOG.info("OffHeapKeyManager | removeStoreInfo |  bucket not existed  -> " + bucketNo);
            return null;
        }
    }

    @Override
    public MdbPointer replaceStoreInfo(byte[] key, MdbPointer newInfo, Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.replace(key, newInfo);
        } else {
            LOG.info("OffHeapKeyManager | replaceStoreInfo |  bucket not existed  -> " + bucketNo);
            return null;
        }
    }

    @Override
    public void resetBuckets(List<BucketInfo> buckets) {
        LOG.info("OffHeapKeyManager | begin reset bucket -> " + buckets + " , existed buckets -> " + bucketsHeapMap.keySet());

        Set<Integer> bucketsNo = bucketsHeapMap.keySet();

        for (Integer bucket : bucketsNo) {
            boolean isContain = false;
            for (BucketInfo info : buckets) {
                if (info.getBucketNo().intValue() == bucket.intValue()) {
                    isContain = true;
                    break;
                }
            }

            if (!isContain) {
                OffHeapMap<byte[], MdbPointer> removeMap = bucketsHeapMap.remove(bucket);
                if (removeMap != null) {
                    //LOG.info("OffHeapKeyManager | remove bucket -> " + bucket);
                    removeMap.clear();
                }
            }
        }

        for (BucketInfo info : buckets) {
            Integer bucketNo = info.getBucketNo();
            bucketsHeapMap.putIfAbsent(bucketNo, new OffHeapMap<byte[], MdbPointer>(this.mdbManager));
        }

        LOG.info("OffHeapKeyManager | after reset buckets -> " + bucketsHeapMap.keySet());
    }

    @Override
    public int getSize(Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.size();
        } else {
            return 0;
        }
    }

    @Override
    public boolean contain(byte[] key, Integer bucketNo) {
        OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        return map != null && map.containsKey(key);
    }
    
    @Override
    public Set<Entry<byte[], MdbPointer>> getEntrySet(Integer bucketNo) {
       /*OffHeapMap<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        return map.entrySet();*/
        throw new UnsupportedOperationException();
    }
}
