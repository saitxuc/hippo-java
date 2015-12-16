package com.test.mdb.impl;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.pinganfu.hippo.mdb.impl.OffHeapMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.mdb.KeyManager;
import com.pinganfu.hippo.mdb.MdbConstants;
import com.pinganfu.hippo.mdb.MdbManager;
import com.pinganfu.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 *
 */
public class OffHeapKeyManagerTest implements KeyManager {
    private static final Logger LOG = LoggerFactory.getLogger(OffHeapKeyManagerTest.class);

    private MdbManager mdbManager;

    private ConcurrentHashMap<Integer, OffHeapMapTest<byte[], MdbPointer>> bucketsHeapMap = new ConcurrentHashMap<Integer, OffHeapMapTest<byte[], MdbPointer>>();

    public OffHeapKeyManagerTest(MdbManager mdbManager, List<BucketInfo> buckets) {
        this.mdbManager = mdbManager;
        for (BucketInfo info : buckets) {
            OffHeapMapTest<byte[], MdbPointer> offHeapMap = new OffHeapMapTest<byte[], MdbPointer>(this.mdbManager);
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
            for (Entry<Integer, OffHeapMapTest<byte[], MdbPointer>> entry : bucketsHeapMap.entrySet()) {
                size += entry.getValue().size();
                System.out.println(entry.getKey() + "-----" + entry.getValue().size());
            }
        }
        return size;
    }

    @Override
    public void setKeyStoreInfo(byte[] key, MdbPointer info, Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            map.put(key, info);
        } else {
            LOG.warn("OffHeapKeyManager | setKeyStoreInfo bucketNo -> " + bucketNo + " not existed!!");
        }
    }

    @Override
    public MdbPointer getStoreInfo(byte[] key, Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.get(key);
        } else {
            LOG.info("OffHeapKeyManager | getStoreInfo |  bucket not existed  -> " + bucketNo);
            return null;
        }

    }

    @Override
    public MdbPointer removeStoreInfo(byte[] key, Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.remove(key);
        } else {
            LOG.info("OffHeapKeyManager | removeStoreInfo |  bucket not existed  -> " + bucketNo);
            return null;
        }
    }

    @Override
    public MdbPointer replaceStoreInfo(byte[] key, MdbPointer newInfo, Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
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
                if (info.getBucketNo() == bucket) {
                    isContain = true;
                    break;
                }
            }

            if (!isContain) {
                OffHeapMapTest<byte[], MdbPointer> removeMap = bucketsHeapMap.remove(bucket);
                if (removeMap != null) {
                    //LOG.info("OffHeapKeyManager | remove bucket -> " + bucket);
                    removeMap.clear();
                }
            }
        }

        for (BucketInfo info : buckets) {
            Integer bucketNo = info.getBucketNo();
            bucketsHeapMap.putIfAbsent(bucketNo, new OffHeapMapTest<byte[], MdbPointer>(this.mdbManager));
        }

        LOG.info("OffHeapKeyManager | after reset buckets -> " + bucketsHeapMap.keySet());
    }

    @Override
    public int getSize(Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        if (map != null) {
            return map.size();
        } else {
            return 0;
        }
    }

    @Override
    public boolean contain(byte[] key, Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        return map != null && map.containsKey(key);
    }

    @Override
    public Set<Entry<byte[], MdbPointer>> getEntrySet(Integer bucketNo) {
        OffHeapMapTest<byte[], MdbPointer> map = bucketsHeapMap.get(bucketNo);
        return map.entrySet();
    }
}
