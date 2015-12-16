package com.pinganfu.hippo.leveldb;

import static com.pinganfu.hippo.common.errorcode.HippoCodeDefine.HIPPO_MDB_DELETE;
import static com.pinganfu.hippo.common.errorcode.HippoCodeDefine.HIPPO_OPER_ERROR;
import static com.pinganfu.hippo.common.errorcode.HippoCodeDefine.HIPPO_OVER_LIMIT;
import static com.pinganfu.hippo.common.errorcode.HippoCodeDefine.HIPPO_SYS_ERROR;
import static com.pinganfu.hippo.common.errorcode.HippoCodeDefine.HIPPO_UNKNOW_ERROR;
import static com.pinganfu.hippo.leveldb.cluster.BConstansts.KEY_BIZ_APP;
import static com.pinganfu.hippo.leveldb.cluster.BConstansts.KEY_BUCKET;
import static com.pinganfu.hippo.leveldb.cluster.BConstansts.KEY_VERSION;
import static com.pinganfu.hippo.leveldb.cluster.DdbFactory.factory;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ArrayListMultimap;
import com.pinganfu.hippo.common.config.PropConfigConstants;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.LimitUtils;
import com.pinganfu.hippo.leveldb.cluster.Ddb;
import com.pinganfu.hippo.leveldb.cluster.Ddb.Position;
import com.pinganfu.hippo.leveldb.cluster.Entrys;
import com.pinganfu.hippo.leveldb.cluster.LdbResult;
import com.pinganfu.hippo.leveldb.impl.InternalKey;
import com.pinganfu.hippo.leveldb.util.Slice;
import com.pinganfu.hippo.leveldb.util.Slices;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.store.StoreEngine;
import com.pinganfu.hippo.store.StoreEngineFactory;
import com.pinganfu.hippo.store.exception.HippoStoreException;
import com.pinganfu.hippo.store.model.GetResult;

/**
 * @author yangxin
 */
public class LevelDbStoreEngine extends LifeCycleSupport implements StoreEngine {
    private final static Logger log = LoggerFactory.getLogger(LevelDbStoreEngine.class);

    private Ddb levelDb;
    private StoreEngine mdb;
    private long maxCapacity = -1;
    private Map<String, String> initParams = null;

    private boolean overflow(int size) {
        if (maxCapacity < 0) {
            return false;
        }
        return (size + levelDb.getCurCapacity()) > maxCapacity;
    }

    public void add(byte[] key, byte[] value, int bucket, int bizApp, int expire, int version) throws HippoStoreException {
        if (overflow(key.length + value.length)) {
            log.error("exceeds levelDb capacity limit maxCapacity is " + maxCapacity / 1024 / 1024 + "m.");
            throw new HippoStoreException("exceeds levelDb capacity limit maxCapacity is " + maxCapacity / 1024 / 1024 + "m.", HIPPO_OVER_LIMIT);
        }

        WriteOptions options = new WriteOptions();
        options.bucket(bucket).bizApp(bizApp).expireTime(expire).version((short) upgrade(version));
        try {
            if (mdb != null) {
                try {
                    mdb.addData(key, value, expireSeconds(expire), 0, upgrade(version));
                } catch (Throwable e) {
                    log.error("Put levelDb'data to mdb error for add.", e);
                }
            }

            levelDb.put(InternalKey.packageKey_w(key, options).getBytes(), value, options);
        } catch (DBException e) {
            handleException(HIPPO_OPER_ERROR, e);
        } catch (Exception e) {
            handleException(HIPPO_UNKNOW_ERROR, e);
        } catch (Throwable e) {
            handleException(HIPPO_SYS_ERROR, e);
        }
    }

    @Override
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo) throws HippoStoreException {
        addData(key, value, expire, bucketNo, KEY_VERSION);
        return true;
    }

    private int expireSeconds(int second) {
        return second < 0 ? 86400/*1 day*/ : second;
    }

    private int timestamp2Seconds(long timestamp) {
        return timestamp < 0 ? 86400/*1 day*/ : (int) (timestamp - System.currentTimeMillis()) / 1000;
    }

    public GetResult get(byte[] key, int bucket, int bizApp, int version) throws HippoStoreException {
        GetResult data = null;
        try {
            if (mdb != null) {
                try {
                    GetResult d = mdb.getData(key);
                    if (d != null && (d.getVersion() == version || version == 0)) {
                        data = d;
                    }
                } catch (Throwable e) {
                    // TODO
                }
            }

            /*fix the bug the expired data in the mdb but not expired in the levelDB
            * begin*/
            if (data != null) {
                if (data.getExpireTime() < System.currentTimeMillis()) {
                    data = null;
                }
            }
            /**
             * end
             */
            if (data == null) {
                ReadOptions options = new ReadOptions();
                options.bucket(bucket).bizApp(bizApp).version(version);
                LdbResult result = levelDb.get0(InternalKey.packageKey_q(key, options).getBytes(), options);
                if (result != null) {
                    if (result.getExpire() < 0) {
                        //not expired forever
                        data = new GetResult(result.getValue(), result.getVersion(), -1L);
                    } else {
                        data = new GetResult(result.getValue(), result.getVersion(), result.getExpire());
                    }
                }
                if (mdb != null && data != null) {
                    try {
                        mdb.addData(key, data.getContent(), timestamp2Seconds(result.getExpire()), 0, result.getVersion());
                    } catch (Throwable e) {
                        log.error("Put levelDb'data to mdb error for get.", e);
                    }
                }
            }
            if (data == null) {
//				data = new GetResult(null, version);
                throw new HippoStoreException(" no find value of key: " + new String(key), HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            }
        } catch (HippoStoreException e) {
            throw e;
        } catch (DBException e) {
            handleException(HIPPO_OPER_ERROR, e);
        } catch (Exception e) {
            handleException(HIPPO_UNKNOW_ERROR, e);
        } catch (Throwable e) {
            handleException(HIPPO_SYS_ERROR, e);
        }
        return data;
    }

    private void handleException(String errorCode, Throwable throwable) throws HippoStoreException {
        log.error("Operate levelDb happen exception, errorCode=" + errorCode, throwable);
        throw new HippoStoreException("errorCode=" + errorCode, errorCode);
    }

    public boolean remove(byte[] key, int bucket, int bizApp, int version) throws HippoStoreException {
        WriteOptions options = new WriteOptions();
        options.bucket(bucket).bizApp(bizApp).version((short) version).expireTime(-1L);
        try {
            if (mdb != null) {
                if (!mdb.removeData(key, 0)) {
                    handleException(HIPPO_MDB_DELETE, null);
                }
            }

            levelDb.delete(InternalKey.packageKey_d(key, options).getBytes(), options);
        } catch (DBException e) {
            handleException(HIPPO_OPER_ERROR, e);
        } catch (HippoStoreException e) {
            throw e;
        } catch (Exception e) {
            handleException(HIPPO_UNKNOW_ERROR, e);
        } catch (Throwable e) {
            handleException(HIPPO_SYS_ERROR, e);
        }
        return true;
    }

    @Override
    public boolean removeData(byte[] key) throws HippoStoreException {
        return remove(key, KEY_BUCKET, KEY_BIZ_APP, KEY_VERSION);
    }

    @Override
    public boolean removeData(byte[] key, int bucketNo) throws HippoStoreException {
        return remove(key, bucketNo, KEY_BIZ_APP, KEY_VERSION);
    }

    @Override
    public boolean removeData(byte[] key, int bucketNo, int version) throws HippoStoreException {
        return remove(key, bucketNo, KEY_BIZ_APP, version);
    }

    @Override
    public int getDataCount() throws HippoStoreException {
        log.warn("UnsupportedOperationException:getDataCount()");
        return -1;
    }

    @Override
    public String getName() {
        return "levelDb";
    }

    @Override
    public long size() {
        log.warn("UnsupportedOperationException:size()");
        return -1;
    }

    @Override
    public long getCurrentUsedCapacity() {
        return levelDb.approximateSize();
    }

    @Override
    public void setExtraInitParams(Map<String, String> params) {
        this.initParams = params;
    }

    private Options options;

    @Override
    public void doInit() {
        if (options == null) {
            if (initParams != null && Boolean.parseBoolean(initParams.get(PropConfigConstants.LEVELDB_MDB_USE_FLAG))) {
                log.info("levelDB param has been set -> mdb in levelDB setting is open!");
                options = new Options().createIfMissing(true).useMdb(true);
            } else {
                log.info("levelDB param has been set -> mdb in levelDB setting is close!");
                options = new Options().createIfMissing(true).useMdb(false);
            }
        }

        if (options.useMdb()) {
            mdb = StoreEngineFactory.findStoreEngine("mdb");
            mdb.setExtraInitParams(initParams);
        }

        long size = 0;
        if (mdb != null) {
            if (initParams != null) {
                String mdbLimit = initParams.get(PropConfigConstants.LEVELDB_MDB_USE_LIMIT);
                size = LimitUtils.calculationLimit(mdbLimit);
            }
            if (size <= 0) {
                throw new IllegalArgumentException("The STORE ENGINE LIMIT NOT RIGHT");
            }
            log.warn("levelDB use Mdb maxDirectMemorySize -> " + size);
            mdb.setLimit(size);
            mdb.init();
        }
    }

    @Override
    public void doStart() {
        try {
            this.levelDb = (Ddb) factory.open(options);

            setBuckets(bucketsMap.get(UserType.MASTER));

            if (mdb != null) {
                mdb.start();

                ((MdbStoreEngine) mdb).startPeriodsExpire();
            }
        } catch (Exception e) {
            log.error("Building level db error!", e);
            throw new RuntimeException(e);
        }
    }

    public static enum UserType {
        MASTER, SLAVE
    }

    private ArrayListMultimap<Object, BucketInfo> bucketsMap = ArrayListMultimap.create();

    /**
     * 主桶初始化,如果levelDb还没有初始化完成，则初始化之后会在{@link #doStart()}中再次调用
     */
    @Override
    public void setBuckets(List<BucketInfo> buckets) {
        setBuckets(buckets, UserType.MASTER);
    }

    /**
     * 用于master和salve调用
     *
     * @param buckets
     * @param type
     */
    public synchronized void setBuckets(List<BucketInfo> buckets, Object type) {
        bucketsMap.removeAll(type);
        bucketsMap.putAll(type, buckets);

        if (levelDb != null) {
            Set<Integer> newBuckets = new HashSet<Integer>();
            for (BucketInfo bi : bucketsMap.values()) {
                newBuckets.add(bi.getBucketNo());
            }

            if (newBuckets.size() > 0) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("reset buckets " + newBuckets);
                    }
                    // 数据清理由master来完成
                    levelDb.setBuckets(newBuckets, type == UserType.MASTER);
                } catch (Throwable e) {
                    log.error("Sweep bucket data error.", e);
                }
            }
        } else {
            log.warn("levelDb is null when reset buckets!");
        }
    }

    @Override
    public void doStop() {
        try {
            factory.close(levelDb);
        } catch (IOException e) {
            log.error("Stoping level db error!", e);
            throw new RuntimeException(e);
        }
        if (mdb != null) {
            mdb.stop();
        }
    }

    @Override
    public void setLimit(long limit) {
        log.warn("LevelDb maxCapacity:" + limit);
        this.maxCapacity = limit;
    }

    @Override
    public List<BucketInfo> getBuckets() {
        log.warn("UnsupportedOperationException:getBuckets()");
        return null;
    }

    @Override
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo, int version) throws HippoStoreException {
        add(key, value, bucketNo, KEY_BIZ_APP, expire, version);
        return true;
    }

    @Override
    public GetResult getData(byte[] key, int bucketNo) throws HippoStoreException {
        return get(key, bucketNo, KEY_BIZ_APP, 0);
    }

    @Override
    public long getBucketUsedCapacity(int bucket) {
        log.warn("UnsupportedOperationException:getBucketUsedCapacity()");
        return -1;
    }

    @Override
    public boolean setBit(byte[] key, int offset, boolean val, int bucketNo, int expire) throws HippoStoreException {
        return false;
    }

    @Override
    public GetResult getBit(byte[] key, int offset, int bucketNo) throws HippoStoreException {
        return null;
    }

    @Override
    public boolean removeBit(byte[] key, int bucketNo) throws HippoStoreException {
        return false;
    }
    
    @Override
    public boolean addData(byte[] key, byte[] value, int expire) throws HippoStoreException {
        add(key, value, KEY_BUCKET, KEY_BIZ_APP, expire, KEY_VERSION);
        return true;
    }

    @Override
    public GetResult getData(byte[] key) throws HippoStoreException {
        return get(key, KEY_BUCKET, KEY_BIZ_APP, KEY_VERSION);
    }

    @Override
    public boolean updateData(byte[] key, byte[] value, int expire) throws HippoStoreException {
        return updateData(key, value, expire, 0);
    }

    @Override
    public boolean updateData(byte[] key, byte[] value, int expire, int version) throws HippoStoreException {
        return updateData(key, value, expire, KEY_BUCKET, version);
    }

    @Override
    public synchronized boolean updateData(byte[] key, byte[] value, int expire, int bucketNo, int version)
            throws HippoStoreException {
        ReadOptions rOptions = new ReadOptions();
        rOptions.bucket(bucketNo).bizApp(KEY_BIZ_APP).version(upgrade(version));
        LdbResult result = levelDb.get0(InternalKey.packageKey_q(key, rOptions).getBytes(), rOptions);
        if (result == null) {
            throw new HippoStoreException(" no find value of key: " + new String(key), HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            //return false;
        } else {
            if (version > 0) {
                // 删除
                WriteOptions wOptions = new WriteOptions();
                wOptions.bucket(bucketNo).bizApp(KEY_BIZ_APP).version((short) version);
                levelDb.delete(InternalKey.packageKey_d(key, wOptions).getBytes(), wOptions);

                // 新增：version+1
                addData(key, value, expire, bucketNo, ++version);
            } else {
                addData(key, value, expire, bucketNo, upgrade(version));
            }
            return true;
        }
    }

    private int upgrade(int version) {
        return version < 1 ? KEY_VERSION : version;
    }

    // --------------------------- for migrate --------------------------------------------------------

    /**
     * 读取master需要备份的数据
     */
    public Entrys get(String standby, int bucket, Position pos, int batchSize) {
        return levelDb.get(standby, bucket, pos, batchSize);
    }

    public long getMaxSeq(int bucket) {
        return levelDb.getMaxSeq(bucket);
    }

    /**
     * 保存master发送过来的数据
     */
    public SyncResult set(byte[] data, int bucket) {
        SyncResult result = new SyncResult(true);
        if (overflow(data.length)) {
            log.error("exceeds levelDb capacity limit maxCapacity is " + maxCapacity / 1024 / 1024 + "m.");
            result.setSuccess(false);
            return result;
        }

        try {
            int size = 0;
            Entrys entries = new Entrys(Slices.wrappedBuffer(data));
            if (entries != null && entries.size() > 0) {
                for (Entry<Slice, Slice> entry : entries.getData()) {
                    try {
                        if (entry.getValue().length() == 0) {
                            levelDb.delete(entry.getKey().getBytes(), new WriteOptions().bucket(bucket));
                        } else {
                            levelDb.put(entry.getKey().getBytes(), entry.getValue().getBytes(), new WriteOptions().bucket(bucket));
                        }
                        size++;
                    } catch (DBException e) {
                        result.setSuccess(false);
                    }
                }
            }
            result.setSize(size);
        } catch (Throwable e) {
            log.error("Save backup data error!", e);
            result.setSuccess(false);
        }
        return result;
    }

    public class SyncResult {
        private boolean success;
        private int size;

        public SyncResult(boolean success) {
            this.success = success;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }

}
