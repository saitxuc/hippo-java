package com.hippo.broker;

import com.hippo.broker.store.StoreConstants;
import com.hippo.client.HippoResult;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.jmx.AnnotatedMBean;
import com.hippo.jmx.StoreAdapterView;
import com.hippo.mdb.MdbStoreEngine;
import com.hippo.store.StoreEngine;
import com.hippo.store.StoreEngineFactory;
import com.hippo.store.exception.HippoStoreException;
import com.hippo.store.model.GetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

import static com.hippo.jmx.BrokerMBeanSupport.createStoreEngineAdapterName;

/**
 * @author saitxuc
 */
public class DefaultCache extends LifeCycleSupport implements Cache {

    protected static final Logger LOG = LoggerFactory.getLogger(DefaultCache.class);

    private final String DEFAULT_TYPE = StoreConstants.DEFAULT_STORE_TYPE;

    protected BrokerService brokerService;
    private long limit = -1l;
    private List<BucketInfo> buckets = null;
    protected StoreEngine storeEngine;
    private Map<String, String> initParams;
    private int bucketLimit = -1;

    public DefaultCache() {
        super();
    }

    public DefaultCache(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public DefaultCache(BrokerService brokerService, StoreEngine storeEngine) {
        this.brokerService = brokerService;
        this.storeEngine = storeEngine;
    }

    @Override
    public void doInit() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache is doing doInit. ");
        }

        if (storeEngine == null) {
            storeEngine = getDefaultEngine();
        }

        storeEngine.setLimit(limit);
        storeEngine.setExtraInitParams(initParams);

        if (buckets == null || buckets.size() == 0) {
            LOG.warn("buckets is null when init the default cache -> init to set the default bucket -> 0");
            buckets = new ArrayList<BucketInfo>();
            BucketInfo info = new BucketInfo(0, false);
            buckets.add(info);
        }

        storeEngine.setBuckets(buckets);

        if (storeEngine instanceof MdbStoreEngine) {
            if (bucketLimit == -1) {
                LOG.warn("bucketLimit does not set when init the default the cache, use the buckets size to set the limit! buckets size is " + buckets.size());
                Set<Integer> set = new HashSet<Integer>();
                for (BucketInfo info : buckets) {
                    set.add(info.getBucketNo());
                }
                ((MdbStoreEngine) storeEngine).setBucketLimit(set.size());
            } else {
                ((MdbStoreEngine) storeEngine).setBucketLimit(bucketLimit);
            }
        }

        storeEngine.init();

        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache finish doInit. ");
        }
    }

    @Override
    public void doStart() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache is doing doStart. ");
        }
        try {
            if (brokerService != null && brokerService.isUseJmx()) {
                StoreAdapterView view = new StoreAdapterView(this.storeEngine);
                view.setDataViewCallable(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return storeEngine.toString();
                    }
                });
                AnnotatedMBean.registerMBean(brokerService.getManagementContext(), view, createStoreEngineAdapterName(brokerService.getBrokerObjectName()
                        .toString(), getClass().getName()));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        storeEngine.start();
        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache finish doStart. ");
        }
    }

    @Override
    public void doStop() {
        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache is doing doStop. ");
        }
        release();
        if (LOG.isInfoEnabled()) {
            LOG.info(" DefaultCache finish doStop. ");
        }
    }

    @Override
    public HippoResult set(int expire, byte[] key, byte[] value, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = false;
            success = storeEngine.addData(key, value, expire, bucketNo);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" set value to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult set(int expire, byte[] key, byte[] value, int version, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = false;
            success = storeEngine.addData(key, value, expire, bucketNo, version);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" set value to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult get(byte[] key, int bucketNo) {
        HippoResult result = null;
        try {
            GetResult getResult = storeEngine.getData(key, bucketNo);
            byte[] value = getResult.getContent();
            result = new HippoResult(true, value, getResult.getVersion(), getResult.getExpireTime());
        } catch (HippoStoreException e) {
            LOG.error(" get key to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }
    
    @Override
    public HippoResult exists(byte[] key, int bucketNo) {
    	 HippoResult result = null;
         try {
             boolean founded = storeEngine.exists(key, bucketNo);
             result = new HippoResult(founded);
         } catch (HippoStoreException e) {
             LOG.error(" get key to cache happen error. ", e);
             result = new HippoResult(false, e.getErrorCode(), key);
         }
         return result;
    }
    
    @Override
    public HippoResult get(byte[] key, int version, int bucketNo) {
        return get(key, bucketNo);
    }

    @Override
    public HippoResult remove(byte[] key, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = storeEngine.removeData(key, bucketNo);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" remove key from cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult remove(byte[] key, int version, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = storeEngine.removeData(key, bucketNo);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" remove key from cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult getBit(byte[] key, int offset, int bucketNo) {
        HippoResult result = null;
        try {
            GetResult getResult = storeEngine.getBit(key, offset, bucketNo);
            result = new HippoResult(true, getResult.getContent(), getResult.getVersion(), getResult.getExpireTime());
        } catch (HippoStoreException e) {
            LOG.error(" get bit from cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult setBit(int expire, byte[] key, int offset, boolean val, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = false;
            success = storeEngine.setBit(key, offset, val, bucketNo, expire);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" set bit to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult updateBit(int expire, byte[] key, int offset, boolean val, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = false;
            success = storeEngine.setBit(key, offset, val, bucketNo, expire);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" update bit to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public HippoResult removeBit(byte[] key, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = storeEngine.removeBit(key, bucketNo);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" remove bit from cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }

    @Override
    public void release() {
        if (storeEngine != null) {
            storeEngine.stop();
        }
    }

    @Override
    public StoreEngine getEngine() {
        //init
        return storeEngine;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public void checkStoreEngineConfig() {
        // TODO Auto-generated method stub

    }

    private StoreEngine getDefaultEngine() {
        StoreEngine storeEngine = StoreEngineFactory.findStoreEngine(DEFAULT_TYPE);
        if (storeEngine == null) {
            throw new RuntimeException("can't find the sender impl class of type[" + DEFAULT_TYPE + "]");
        }
        return storeEngine;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setBuckets(List<BucketInfo> buckets) {
        this.buckets = buckets;
    }

    public void setStoreEngine(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }

    @Override
    public void setBucketLimit(int bucketLimit) {
        this.bucketLimit = bucketLimit;
    }

    @Override
    public void setInitParams(Map<String, String> params) {
        this.initParams = params;
    }

    @Override
    public HippoResult update(int expire, byte[] key, byte[] value, int version, int bucketNo) {
        HippoResult result = null;
        try {
            boolean success = false;
            success = storeEngine.updateData(key, value, expire, bucketNo, version);
            result = new HippoResult(success);
        } catch (HippoStoreException e) {
            LOG.error(" update value to cache happen error. ", e);
            result = new HippoResult(false, e.getErrorCode(), key);
        }
        return result;
    }
}
