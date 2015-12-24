package com.hippo.mdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hippo.common.Extension;
import com.hippo.common.config.PropConfigConstants;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.common.util.LimitUtils;
import com.hippo.mdb.impl.MdbCapacityController;
import com.hippo.mdb.impl.MdbManagerImpl;
import com.hippo.mdb.obj.MdbBaseOper;
import com.hippo.store.StoreEngine;
import com.hippo.store.TransDataCallBack;
import com.hippo.store.exception.HippoStoreException;
import com.hippo.store.model.GetResult;

/**
 * @author saitxuc
 *         write 2014-7-25
 */
@Extension("mdb")
public class MdbStoreEngine extends LifeCycleSupport implements StoreEngine, Runnable {

    protected static final Logger LOG = LoggerFactory.getLogger(MdbStoreEngine.class);

    private Map<String, String> initParams = null;

    private MdbManager mdbManager = null;

    private long limit = -1L;

    private int bucketLimit = -1;

    private List<BucketInfo> buckNos;

    private ScheduledExecutorService scheduledExecutorService;

    private AtomicBoolean expireStarted = new AtomicBoolean(false);

    private int bitSizePer = 32 * 1024;

    public MdbStoreEngine() {
    }

    @Override
    public boolean addData(byte[] key, byte[] value, int expire) throws HippoStoreException {
        return this.addData(key, value, expire, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo) throws HippoStoreException {
        return addData(key, value, expire, bucketNo, MdbConstants.DEFAULT_VERSION);
    }

    @Override
    public GetResult getData(byte[] key) throws HippoStoreException {
        return this.getData(key, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public GetResult getData(byte[] key, int bucketNo) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerOper(key, null, -1, bucketNo, MdbConstants.DEFAULT_VERSION, MdbBaseOper.GET_OPER);
        return new GetResult(mdbResult.getValue(), mdbResult.getVersion(), mdbResult.getExpireTime());
    }
    
	@Override
	public boolean exists(byte[] key) throws HippoStoreException {
		return this.exists(key, MdbConstants.DEFAULT_BUCKET_NO);
	}
    
	@Override
	public boolean exists(byte[] key, int bucketNo)
			throws HippoStoreException {
		MdbResult mdbResult = mdbManager.offerOper(key, null, -1, bucketNo, MdbConstants.DEFAULT_VERSION, MdbBaseOper.EXIISTS_OPER);
        return mdbResult.isSuccess();
	}
	
    @Override
    public boolean removeData(byte[] key) throws HippoStoreException {
        return this.removeData(key, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public boolean removeData(byte[] key, int bucketNo) throws HippoStoreException {
        return removeData(key, bucketNo, MdbConstants.DEFAULT_VERSION);
    }

    @Override
    public boolean updateData(byte[] key, byte[] value, int expire) throws HippoStoreException {
        return updateData(key, value, expire, MdbConstants.DEFAULT_BUCKET_NO);
    }

    @Override
    public boolean updateData(byte[] key, byte[] value, int expire, int bucketNo) throws HippoStoreException {
        return updateData(key, value, expire, bucketNo, MdbConstants.DEFAULT_VERSION);
    }

    @Override
    public int getDataCount() throws HippoStoreException {
        return mdbManager.countDB();
    }

    @Override
    public boolean isStarted() {
        return mdbManager != null && mdbManager.isStarted();
    }

    @Override
    public void doInit() {
        LOG.info("MdbStoreEngine begin to do doInit");
        boolean isNeedBucketLimit = true;
        if (buckNos == null || buckNos.size() == 0) {
            LOG.warn("The STORE ENGINE BUCKET IS EMPTY!!!!!!!!!!!!,we will use default bucket 0");
            if (buckNos == null) {
                buckNos = new ArrayList<BucketInfo>();
            }

            BucketInfo info = new BucketInfo(0, false);
            buckNos.add(info);

            if (bucketLimit <= 1) {
                isNeedBucketLimit = false;
                LOG.error("The STORE ENGINE BUCKET COUNT LIMIT NOT SET!!!!!!!!!!!! USE DEFAULT VALUE , WILL NOT USE BUCKET LIMIT!");
            } else {
                LOG.info("The STORE ENGINE BUCKET COUNT LIMIT SET IS " + bucketLimit);
            }
        } else {
            LOG.info("ATTENTION: Store engine init the new mdbManager, buckets is " + StringUtils.join(buckNos.toArray(), ","));
            if (bucketLimit == -1) {
                LOG.error("The STORE ENGINE BUCKET COUNT LIMIT NOT SET!!!!!!!!!!!!");
                throw new IllegalArgumentException("The STORE ENGINE BUCKET COUNT LIMIT NOT SET");
            } else {
                LOG.info("The STORE ENGINE BUCKET COUNT LIMIT SET IS " + bucketLimit);
            }
        }

        if (limit == -1) {
            LOG.warn("The STORE ENGINE LIMIT IS NOT SET !!!!!!!!!!!! USE DEFAULT LIMIT 1G");
            limit = MdbConstants.SIZE_1G;
        } else {
            LOG.info("ATTENTION: Store engine init limit is " + limit);
        }

        int doExpireCountLimit;
        if (initParams == null) {
            LOG.warn("The STORE ENGINE DOEXPIRECOUNTLIMIT IS NOT SET !!!!!!!!!!!! USE DEFAULT LIMIT 512");
            doExpireCountLimit = 512;
        } else {
            String expireLimitStr = initParams.get(PropConfigConstants.STORE_EXPIRE_COUNT_LIMIT);
            if (!StringUtils.isEmpty(expireLimitStr)) {
                doExpireCountLimit = Integer.parseInt(expireLimitStr);
                if (doExpireCountLimit < 1) {
                    throw new RuntimeException(" db limit set Illegal, please check! ");
                }
            } else {
                doExpireCountLimit = 512;
            }
            LOG.info("ATTENTION: Store engine init doExpireCountLimit is " + doExpireCountLimit);
        }

        float lruFate;
        if (initParams == null) {
            lruFate = 0.1F;
            LOG.warn("The STORE ENGINE LRUFATE IS NOT SET !!!!!!!!!!!! USE DEFAULT FATE 0.1");
        } else {
            String lruFateStr = initParams.get(PropConfigConstants.STORE_LRU_FATE);
            if (!StringUtils.isEmpty(lruFateStr)) {
                lruFate = Float.parseFloat(lruFateStr);
                if (lruFate < 0 || lruFate > 1) {
                    throw new RuntimeException(" db lru fate set Illegal, please check! ");
                }
            } else {
                lruFate = 0.1F;
            }
            LOG.info("ATTENTION: Store engine init lruFate is " + lruFate);
        }

        List<Double> sizeTypes = new ArrayList<Double>();

        if (initParams != null && !StringUtils.isEmpty(initParams.get(PropConfigConstants.STORE_DATA_SIZE_TYPE))) {
            String[] types = StringUtils.split(initParams.get(PropConfigConstants.STORE_DATA_SIZE_TYPE), ",");
            assert types != null;
            for (String size : types) {
                sizeTypes.add(Double.parseDouble(size));
            }
        } else {
            LOG.warn("The STORE ENGINE SIZE TYPES ARE NOT SET !!!!!!!!!!!! use default size type!!");
            for (double index = 0;; index++) {
                Double size = Math.pow(MdbConstants.SIZE_FACTOR, index);
                if (size > MdbConstants.SIZE_LIMIT) {
                    break;
                } else {
                    sizeTypes.add(size);
                }
            }
        }

        if (initParams != null && !StringUtils.isEmpty(initParams.get(PropConfigConstants.STORE_BIT_SIZE_TYPE))) {
            String size = initParams.get(PropConfigConstants.STORE_BIT_SIZE_TYPE);
            long tmp = LimitUtils.calculationLimit(size);
            if (tmp > 0 && tmp < MdbConstants.CAPACITY_SIZE) {
                bitSizePer = (int) tmp;
            } else {
                LOG.warn("The STORE ENGINE BIT SIZE TYPE IS OUT OF LIMIT !!!!!!!!!!!! use default size -> " + bitSizePer);
            }
        } else {
            LOG.warn("The STORE ENGINE BIT SIZE TYPE ARE NOT SET !!!!!!!!!!!! use default size -> " + bitSizePer);
        }

        BlockSizeMapping mapping = new BlockSizeMapping(sizeTypes);
        mdbManager = new MdbManagerImpl(buckNos, limit, doExpireCountLimit, lruFate, bucketLimit, mapping, isNeedBucketLimit, bitSizePer);
        ((MdbManagerImpl) mdbManager).setCapacityController(new MdbCapacityController());
        LOG.info("MdbStoreEngine finished doing doInit");
    }

    @Override
    public void doStart() {
        LOG.info("MdbStoreEngine begin to do doStart");
        mdbManager.start();
        LOG.info("MdbStoreEngine finished doing doStart");
    }

    @Override
    public void doStop() {
        LOG.info("MdbStoreEngine begin to do doStop");
        stopPeriodsExpire();
        mdbManager.stop();
        LOG.info("MdbStoreEngine finished doing doStop");
    }

    @Override
    public String getName() {
        return "mdb";
    }

    @Override
    public long size() {
        return mdbManager.getSize();
    }

    @Override
    public long getCurrentUsedCapacity() {
        return mdbManager.getCurrentUsedCapacity();
    }

    @Override
    public void setExtraInitParams(Map<String, String> params) {
        this.initParams = params;
    }

    @Override
    public long getBucketUsedCapacity(int bucket) {
        return mdbManager.getBucketUsedCapacity(bucket);
    }

    @Override
    public boolean setBit(byte[] key, int offset, boolean val, int bucketNo, int expire) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerBitOper(key, offset, expire, bucketNo, 0, MdbBaseOper.BITSET_OPER, val);
        return mdbResult.isSuccess();
    }

    @Override
    public GetResult getBit(byte[] key, int offset, int bucketNo) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerBitOper(key, offset, -1, bucketNo, 0, MdbBaseOper.BITGET_OPER, false);
        return new GetResult(mdbResult.getValue(), mdbResult.getVersion(), mdbResult.getExpireTime());
    }

    @Override
    public boolean removeBit(byte[] key, int bucketNo) throws HippoStoreException {
        return false;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void run() {
        try {
            mdbManager.doExpireAndGetDeletedDBIfs();
        } catch (Exception e) {
            LOG.error("error when do expire", e);
        }
    }

    public Object getMdbManager() {
        return mdbManager;
    }

    public void setMdbManager(Object mdbManager) {
        this.mdbManager = (MdbManager) mdbManager;
    }

    public void startPeriodsExpire() {
        if (expireStarted.compareAndSet(false, true)) {
            scheduledExecutorService = ExcutorUtils.startSchedule("hippo-expire task ", this, 5000, 5000);
            LOG.info("The expire thread control by MdbStoreEngine started");
        }
    }

    public void stopPeriodsExpire() {
        if (expireStarted.compareAndSet(true, false) && scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            LOG.info("The expire thread control by MdbStoreEngine stoped");
        }
    }

    public List<TransDataCallBack> getDataTransCallBackList() {
        return mdbManager.getDataTransCallBackList();
    }

    public void setDataTransCallBackList(List<TransDataCallBack> callbacks) {
        mdbManager.setDataTransCallBackList(callbacks);
    }

    public long getBucketLatestModifiedTime(int bucketNo, boolean useDefault) {
        return mdbManager.getBucketLatestModifiedTime(bucketNo, useDefault);
    }

    @Override
    public void setBuckets(List<BucketInfo> buckets) {
        this.buckNos = buckets;
    }

    @Override
    public List<BucketInfo> getBuckets() {
        return buckNos;
    }

    public void setBucketLimit(int bucketLimit) {
        this.bucketLimit = bucketLimit;
    }

    @Override
    public boolean addData(byte[] key, byte[] value, int expire, int bucketNo, int version) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerOper(key, value, expire, bucketNo, version, MdbBaseOper.ADD_OPER);
        return mdbResult.isSuccess();
    }

    @Override
    public boolean removeData(byte[] key, int bucketNo, int version) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerOper(key, null, -1, bucketNo, version, MdbBaseOper.REMOVE_OPER);
        return mdbResult.isSuccess();
    }

    @Override
    public boolean updateData(byte[] key, byte[] value, int expire, int bucketNo, int version) throws HippoStoreException {
        MdbResult mdbResult = mdbManager.offerOper(key, value, expire, bucketNo, version, MdbBaseOper.UPDATE_OPER);
        return mdbResult.isSuccess();
    }

}
