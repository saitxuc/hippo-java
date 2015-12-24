package com.hippo.mdb.impl;

import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED;
import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST;
import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_OPERATION_VERSION_WRONG;
import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_PROTOCOL_ERROR;
import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_SERVER_ERROR;
import static com.hippo.common.errorcode.HippoCodeDefine.HIPPO_SIZE_NOT_EXISTED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.SyncDataTask;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.common.serializer.Serializer;
import com.hippo.common.util.HashUtil;
import com.hippo.common.util.Logarithm;
import com.hippo.mdb.BlockSizeMapping;
import com.hippo.mdb.CapacityController;
import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.DBAssembleManager;
import com.hippo.mdb.KeyManager;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbManager;
import com.hippo.mdb.MdbOper;
import com.hippo.mdb.MdbResult;
import com.hippo.mdb.exception.OutOfMaxCapacityException;
import com.hippo.mdb.obj.DBInfo;
import com.hippo.mdb.obj.MdbAddOper;
import com.hippo.mdb.obj.MdbBaseOper;
import com.hippo.mdb.obj.MdbBitGetOper;
import com.hippo.mdb.obj.MdbBitUpdateOper;
import com.hippo.mdb.obj.MdbGetOper;
import com.hippo.mdb.obj.MdbOperResult;
import com.hippo.mdb.obj.MdbPointer;
import com.hippo.mdb.obj.MdbRemoveOper;
import com.hippo.mdb.obj.MdbUpdateOper;
import com.hippo.mdb.utils.BufferUtil;
import com.hippo.store.TransDataCallBack;
import com.hippo.store.exception.HippoStoreException;

/**
 * @author saitxuc
 *         write 2014-7-28
 */
public class MdbManagerImpl extends LifeCycleSupport implements MdbManager {
    private static final int MUTEX_ARRAY_SIZE = 1000;

    private Object[] counterMutex = new Object[MUTEX_ARRAY_SIZE];

    private static final Logger LOG = LoggerFactory.getLogger(MdbManagerImpl.class);
    private static Map<String, Serializer> serializerMap = new HashMap<String, Serializer>();
    private final ConcurrentHashMap<String, DBAssembleManager> dbAssembleMap = new ConcurrentHashMap<String, DBAssembleManager>();
    private final ConcurrentHashMap<Integer, AtomicLong> bucketLimits = new ConcurrentHashMap<Integer, AtomicLong>();
    private final AtomicBoolean full = new AtomicBoolean(false);
    private final AtomicBoolean expiring = new AtomicBoolean(false);
    private final AtomicInteger curPosition = new AtomicInteger();
    private final ReentrantLock lock = new ReentrantLock();
    private final Object resetMutex = new Object();
    private final Object expiringMutex = new Object();

    static {
        //load all the support serializers
        ServiceLoader<Serializer> serializers = ServiceLoader.load(Serializer.class);
        for (Serializer serializer : serializers) {
            serializerMap.put(serializer.getName(), serializer);
        }
    }

    private List<BucketInfo> bucketNos = new ArrayList<BucketInfo>();
    private KeyManager keyManager = null;
    private String serializerType = null;
    private byte[] separator = null;
    private long limit = 0L;
    private int expireLimit = 0;
    private float lruFate = 0F;
    private volatile long allCapacity = 0;
    private boolean usingBucketLimit = false;
    private volatile long eachCapacity = 0;
    private List<TransDataCallBack> callbacks = null;
    private CapacityController capacityController;
    private BlockSizeMapping sizeMapping;
    private int bucketLimit;
    private int bitSizePer = 0;
    private int usedByteSize = 0;

    public MdbManagerImpl(List<BucketInfo> initBucketNos, long limit, int expireLimit, float lruFate, int bucketLimit, BlockSizeMapping mapping,
                          boolean usingBucketLimit, int bitSizePer) {
        bucketNos.addAll(initBucketNos);
        this.usingBucketLimit = usingBucketLimit;
        this.bucketLimit = bucketLimit;
        this.limit = limit;
        this.expireLimit = expireLimit;
        this.lruFate = lruFate;
        this.sizeMapping = mapping;
        this.bitSizePer = bitSizePer;
    }

    @Override
    public void doInit() {
        for (int i = 0; i < counterMutex.length; i++) {
            counterMutex[i] = new Object();
        }
        
        if (StringUtils.isEmpty(serializerType)) {
            serializerType = MdbConstants.DEFAULT_SERIALIZER_TYPE;
        }

        Serializer serializer = createSerializer(serializerType);

        try {
            separator = serializer.serialize(":");
            usedByteSize = MdbConstants.HEADER_LENGTH_FOR_INT * 3 + separator.length * 2 + 8;
        } catch (IOException e) {
            LOG.error("IOException when serialize separator", e);
        }

        if (keyManager == null) {
            keyManager = new OffHeapKeyManager(this, bucketNos);
        }

        if (usingBucketLimit && bucketNos != null) {
            for (BucketInfo info : bucketNos) {
                int bucket = info.getBucketNo();
                bucketLimits.put(bucket, new AtomicLong(0));
            }

            this.eachCapacity = limit / this.bucketLimit;
            LOG.info("cal eachCapacity finished, each for every bucketNo is " + eachCapacity + " ,bucketLimit is " + bucketLimit);
        }

        for (Double sizeFlag : sizeMapping.getSizeTypes()) {
            String sizeFlagString = sizeFlag + "";
            LOG.info(String.format("new DBAssembleInfo created, size is %s K!!", sizeFlagString));
            dbAssembleMap.put(sizeFlagString, new DBAssembleManagerImpl(bucketNos, sizeFlagString, this, keyManager, sizeMapping, expireLimit));
        }

        if (capacityController != null) {
            if (capacityController instanceof MdbCapacityController) {
                ((MdbCapacityController) capacityController).setMdbmanager(this);
            }
            capacityController.init();
        }
    }

    @Override
    public void doStart() {
        for (DBAssembleManager dbAssembleManager : dbAssembleMap.values()) {
            dbAssembleManager.start();
        }

        if (capacityController != null) {
            capacityController.start();
            LOG.info("capacityController start!!");
        }
    }

    @Override
    public void doStop() {
        if (capacityController != null) {
            capacityController.stop();
        }

        for (DBAssembleManager dbAssembleManager : dbAssembleMap.values()) {
            dbAssembleManager.stop();
        }

        dbAssembleMap.clear();
    }

    public DBAssembleManager getDBAssembleManager(String sizeKey) {
        return dbAssembleMap.get(sizeKey);
    }

    public ConcurrentHashMap<String, DBAssembleManager> getDbAssembleMap() {
        return dbAssembleMap;
    }

    @Override
    public MdbResult offerOper(byte[] key, byte[] value, int expire, Integer bucketNo, int version, String operAction) throws HippoStoreException {
        MdbResult mdbResult;
        if (operAction.equals(MdbBaseOper.ADD_OPER)) {
            mdbResult = handleAdd(key, value, expire, bucketNo, version);
        } else if (operAction.equals(MdbBaseOper.GET_OPER)) {
            mdbResult = handleGet(key, bucketNo);
        } else if (operAction.equals(MdbBaseOper.EXIISTS_OPER)) {
            mdbResult = handleExists(key, bucketNo);
        } else if (operAction.equals(MdbBaseOper.UPDATE_OPER)) {
            mdbResult = handleUpdate(key, value, expire, bucketNo, version);
        } else if (operAction.equals(MdbBaseOper.REMOVE_OPER)) {
            mdbResult = handleRemove(key, bucketNo, version);
        } else {
            mdbResult = new MdbResult(key, false, HIPPO_PROTOCOL_ERROR);
        }

        if (mdbResult == null) {
            mdbResult = new MdbResult(key, false);
        }
        return mdbResult;
    }

    @Override
    public MdbResult offerBitOper(byte[] key, int offset, int expire, int bucketNo, int version, String bitsetOper, boolean val) throws HippoStoreException {
        MdbResult mdbResult = null;

        if (bitsetOper.equals(MdbBaseOper.BITSET_OPER)) {
            mdbResult = handleBitSet(key, offset, bucketNo, version, val, expire);
        }

        if (bitsetOper.equals(MdbBaseOper.BITGET_OPER)) {
            mdbResult = handleBitGet(key, offset, bucketNo, version);
        }

        if (mdbResult == null) {
            mdbResult = new MdbResult(key, false);
        }

        return mdbResult;
    }

    private MdbResult handleBitGet(final byte[] key, int offset, int bucketNo, int version) throws HippoStoreException {
        if (offset < 0) {
            throw new HippoStoreException("bit out of range , not supported");
        }

        MdbResult result = null;

        try {
            MdbPointer sinfo = keyManager.getStoreInfo(key, bucketNo);

            if (sinfo == null) {
                throw new HippoStoreException(" no find value of key: " + new String(key), HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            }

            int byteSizeLeft = bitSizePer - usedByteSize - key.length;

            int bitSizeLeft = byteSizeLeft * 8;

            String sizeKey = BufferUtil.getSizePeriod(sinfo.getLength(), sizeMapping);

            if (StringUtils.isEmpty(sizeKey)) {
                throw new HippoStoreException(String.format("do not find the size type, length is %d", sinfo.getLength()), HippoCodeDefine.HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(sizeKey);

            if (manager == null) {
                throw new HippoStoreException("find no DBAssembleManager, size not contain in the size mapping! wanted size is " + sizeKey, HippoCodeDefine.HIPPO_SERVER_ERROR);
            }

            int offsetInBlock = offset % bitSizeLeft;
            int byteIndex = offsetInBlock >> 3;
            int offsetInByte = offsetInBlock & 0x7;

            MdbBitGetOper mdbGetOper = new MdbBitGetOper(bucketNo, sinfo, key, byteIndex, offsetInByte, null);

            MdbOperResult oresult = manager.handleOper(mdbGetOper);

            if (StringUtils.isNotEmpty(oresult.getErrorCode())) {
                throw new HippoStoreException("handleGet error", oresult.getErrorCode());
            }

            result = new MdbResult(oresult.getContent(), oresult.getVersion());

            result.setExpireTime(oresult.getExpireTime());

            return result;
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HippoCodeDefine.HIPPO_SERVER_ERROR);
            }
        }
    }

    private MdbResult handleBitUpdate(final byte[] key, int offset, final Integer bucketNo, int version, boolean val, int expire, final MdbPointer sInfo) throws HippoStoreException {

        if (offset < 0 || offset > MdbConstants.MAX_HIPPO_OFFSET) {
            throw new HippoStoreException("bit out of range , not supported", HippoCodeDefine.HIPPO_DATA_OUT_RANGE);
        }

        int byteSizeLeft = bitSizePer - usedByteSize - key.length;

        int bitSizeLeft = byteSizeLeft * 8;

        final byte[] expireTime = Logarithm.putLong(produceExpireTime(expire));

        try {
            if (sInfo == null) {
                throw new HippoStoreException(" no find value of key: " + new String(key), HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            }

            String sizeKey = BufferUtil.getSizePeriod(sInfo.getLength(), sizeMapping);

            if (StringUtils.isEmpty(sizeKey)) {
                throw new HippoStoreException(String.format("do not find the size type, length is %d", sInfo.getLength()), HippoCodeDefine.HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(sizeKey);

            if (manager == null) {
                throw new HippoStoreException("find no DBAssembleManager, size not contain in the size mapping! wanted size is " + sizeKey, HippoCodeDefine.HIPPO_SERVER_ERROR);
            }

            MdbBitUpdateOper oper = new MdbBitUpdateOper(sInfo, offset, key, bitSizeLeft, val, separator.length, version, expireTime, new CompleteCallback() {
                @Override
                public void doComplete(MdbPointer info, long modifiedTime) {
                    if (info != null) {
                        if (callbacks != null) {
                            //byte[] afterJoin = DBInfoUtil.combileByteArrays(key, content);
                            for (TransDataCallBack callBack : callbacks) {
                                callBack.updateModifiedTimeCallBack(bucketNo, modifiedTime);
                            }
                        }
                    }
                }
            });

            MdbOperResult operResult = manager.handleOper(oper);

            if (StringUtils.isNotEmpty(operResult.getErrorCode())) {
                throw new HippoStoreException("handleUpdate error", operResult.getErrorCode());
            }

            oper.complete(operResult.getMdbPointer(), operResult.getModifiedTime());

            return new MdbResult(key, true);
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HippoCodeDefine.HIPPO_SERVER_ERROR);
            }
        }
    }

    private MdbResult handleBitSet(byte[] key, int offset, final Integer bucketNo, int version, boolean val, int expire) throws HippoStoreException {
        /*|keylength|version|contentLength|key|SP|value|SP|expire
         *             12                 |    2        2    8
         *
         */
        if (offset > MdbConstants.MAX_HIPPO_OFFSET || offset < 0) {
            throw new HippoStoreException("bit out of range , not supported", HippoCodeDefine.HIPPO_DATA_OUT_RANGE);
        }
        MdbPointer sInfo = null;
        MdbResult result = null;

        synchronized (getCounterMutex(key)) {
            //double check
            sInfo = keyManager.getStoreInfo(key, bucketNo);
            if (sInfo == null) {
                int byteSizeLeft = bitSizePer - usedByteSize - key.length;
                byte[] data = new byte[byteSizeLeft];
                int bitSizeLeft = (byteSizeLeft) * 8;

                if (val) {
                    int offsetInBlock = offset % bitSizeLeft;
                    int byteIndex = offsetInBlock >> 3;
                    int offsetInByte = offsetInBlock & 0x7;
                    data[byteIndex] |= (1 << (7 - offsetInByte));
                }
                result = handleAdd(key, data, expire, bucketNo, version);
            }
        }

        if (result == null) {
            result = handleBitUpdate(key, offset, bucketNo, version, val, expire, sInfo);
        }

        return result;
    }

    private MdbResult handleAdd(final byte[] key, final byte[] value, int expire, final Integer buck_no, final int version) throws HippoStoreException {
        MdbResult result = new MdbResult(key, true);

        try {
            final long expireTime = produceExpireTime(expire);
            final byte[] contentAfterCombine = BufferUtil.composite(key, value, Logarithm.putLong(expireTime), separator);
            final int length = contentAfterCombine.length + MdbConstants.HEADER_LENGTH_FOR_INT * 3;
            final String sizeFlag = BufferUtil.getSizePeriod(length, sizeMapping);

            if (StringUtils.isEmpty(sizeFlag)) {
                throw new HippoStoreException("do not find the size type, length is " + length, HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(sizeFlag);

            MdbAddOper mdbAddOper = new MdbAddOper(sizeFlag, expireTime, contentAfterCombine, key.length, buck_no, version, new CompleteCallback() {
                @Override
                public void doComplete(MdbPointer info, long modifiedTime) {
                    if (info != null) {
                        //set the dbIfo
                        info.setKLength(key.length);
                        info.setLength(length);

                        //set the key to the map
                        keyManager.setKeyStoreInfo(key, info, buck_no);

                        if (callbacks != null) {
                            for (TransDataCallBack callBack : callbacks) {
                                callBack.updateModifiedTimeCallBack(buck_no, modifiedTime);
                            }
                        }
                    }
                }
            });

            if (manager == null) {
                throw new HippoStoreException("no DBAssembleManager, wanted size is " + sizeFlag, HIPPO_SERVER_ERROR);
            }

            MdbOperResult operResult = manager.handleOper(mdbAddOper);

            if (StringUtils.isNotEmpty(operResult.getErrorCode())) {
                throw new HippoStoreException("error when add data", operResult.getErrorCode());
            }

            mdbAddOper.complete(operResult.getMdbPointer(), operResult.getModifiedTime());

            return result;
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HIPPO_SERVER_ERROR);
            }
        }
    }

    private MdbResult handleGet(final byte[] key, Integer buck_no) throws HippoStoreException {
        try {
            MdbPointer sInfo = keyManager.getStoreInfo(key, buck_no);

            if (sInfo == null) {
                throw new HippoStoreException(" no find value of key: " + new String(key), HIPPO_DATA_DOES_NOT_EXIST);
            }

            String sizeKey = BufferUtil.getSizePeriod(sInfo.getLength(), sizeMapping);

            if (StringUtils.isEmpty(sizeKey)) {
                throw new HippoStoreException(String.format("do not find the size type, length is %d", sInfo.getLength()), HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(sizeKey);

            if (manager == null) {
                throw new HippoStoreException("find no DBAssembleManager, size not contain in the size mapping! wanted size is " + sizeKey, HIPPO_SERVER_ERROR);
            }

            MdbOperResult operResult = manager.handleOper(new MdbGetOper(buck_no, sInfo, key, null));

            if (StringUtils.isNotEmpty(operResult.getErrorCode())) {
                throw new HippoStoreException("handleGet error", operResult.getErrorCode());
            }

            MdbResult result = new MdbResult(operResult.getContent(), operResult.getVersion());

            result.setExpireTime(operResult.getExpireTime());

            return result;
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HIPPO_SERVER_ERROR);
            }
        }
    }
    
    private MdbResult handleExists(final byte[] key, Integer buck_no) throws HippoStoreException {
    	 try {
    		 MdbPointer sInfo = keyManager.getStoreInfo(key, buck_no);
    		 boolean found = false;
             if (sInfo != null) {
            	 found = true;
             }	
             return new MdbResult(key, found);
    	 }catch (Exception e) {
             if (e instanceof HippoStoreException) {
                 throw (HippoStoreException) e;
             } else {
                 LOG.error(e.getMessage(), e);
                 throw new HippoStoreException(e.getMessage(), HIPPO_SERVER_ERROR);
             }
         }
    }
    
    private MdbResult handleUpdate(final byte[] key, final byte[] value, final int expire, final Integer buck_no, final int version) throws HippoStoreException {
        try {
            final MdbPointer sInfoOld = keyManager.getStoreInfo(key, buck_no);

            if (sInfoOld == null) {
                throw new HippoStoreException(" update could not find value of key: " + new String(key), HIPPO_DATA_DOES_NOT_EXIST);
            }

            final int oldVersion = sInfoOld.getVersion();

            if (version != 0 && oldVersion != version) {
                throw new HippoStoreException("version has been changed!", HIPPO_OPERATION_VERSION_WRONG);
            }

            final long expireTime = produceExpireTime(expire);
            final byte[] contentAfterCombine = BufferUtil.composite(key, value, Logarithm.putLong(expireTime), separator);

            final String oldSizeFlag = BufferUtil.getSizePeriod(sInfoOld.getLength(), sizeMapping);

            if (StringUtils.isEmpty(oldSizeFlag)) {
                throw new HippoStoreException(String.format("do not find the old data size type, length is %d", sInfoOld.getLength(), HIPPO_SIZE_NOT_EXISTED));
            }

            final int length = contentAfterCombine.length + MdbConstants.HEADER_LENGTH_FOR_INT * 3;

            final String newSizeFlag = BufferUtil.getSizePeriod(length, sizeMapping);

            if (StringUtils.isEmpty(newSizeFlag)) {
                throw new HippoStoreException(String.format("do not find the new data size type, length is %d", length), HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(newSizeFlag);

            if (manager == null) {
                throw new HippoStoreException("find no DBAssembleManager, size not contain in the size mapping! wanted size is " + newSizeFlag, HIPPO_SERVER_ERROR);
            }

            MdbOper mdbOper;

            if (!newSizeFlag.equals(oldSizeFlag)) {
                //add the new data to the new bucketNo and expire the old data in the old bucketNo
                int newVersion;

                if (version == 0) {
                    newVersion = oldVersion;
                } else {
                    newVersion = oldVersion + 1;
                }

                MdbGetOper mdbGetOper = new MdbGetOper(buck_no, sInfoOld, key, null);

                DBAssembleManager oldManager = dbAssembleMap.get(oldSizeFlag);

                MdbOperResult getResult = oldManager.handleOper(mdbGetOper);

                if (StringUtils.isNotEmpty(getResult.getErrorCode())) {
                    throw new HippoStoreException("handleUpdate error", getResult.getErrorCode());
                }

                if (System.currentTimeMillis() > getResult.getExpireTime()) {
                    throw new HippoStoreException("handleUpdate error, the data has been expired!!", HippoCodeDefine.HIPPO_DATA_EXPIRED);
                }

                mdbOper = new MdbAddOper(newSizeFlag, expireTime, contentAfterCombine, key.length, buck_no, newVersion, new CompleteCallback() {
                    @Override
                    public void doComplete(MdbPointer info, long modifiedTime) {
                        if (info != null) {
                            info.setKLength(key.length);
                            info.setLength(length);

                            keyManager.setKeyStoreInfo(key, info, buck_no);

                            if (callbacks != null) {
                                //byte[] afterJoin = DBInfoUtil.combileByteArrays(key, content);
                                for (TransDataCallBack callBack : callbacks) {
                                    callBack.updateModifiedTimeCallBack(buck_no, modifiedTime);
                                }
                            }
                        }
                    }
                });
            } else {
                //just update the data
                mdbOper = new MdbUpdateOper(sInfoOld, buck_no, contentAfterCombine, oldSizeFlag, expireTime, version, new CompleteCallback() {
                    @Override
                    public void doComplete(MdbPointer info, long modifiedTime) {
                        if (info != null) {
                            info.setKLength(key.length);
                            info.setLength(length);
                            if (callbacks != null) {
                                //byte[] afterJoin = DBInfoUtil.combileByteArrays(key, content);
                                for (TransDataCallBack callBack : callbacks) {
                                    callBack.updateModifiedTimeCallBack(buck_no, modifiedTime);
                                }
                            }
                        }

                    }
                });
            }

            MdbOperResult operResult = manager.handleOper(mdbOper);

            if (StringUtils.isNotEmpty(operResult.getErrorCode())) {
                throw new HippoStoreException("handleUpdate error", operResult.getErrorCode());
            }

            mdbOper.complete(operResult.getMdbPointer(), operResult.getModifiedTime());

            return new MdbResult(key, true);
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HIPPO_SERVER_ERROR);
            }
        }
    }

    private MdbResult handleRemove(final byte[] key, final Integer buck_no, final int version) throws HippoStoreException {
        try {
            final MdbPointer sInfoOld = keyManager.getStoreInfo(key, buck_no);

            if (version != 0 && sInfoOld.getVersion() != version) {
                throw new HippoStoreException("version has been changed!", HIPPO_OPERATION_VERSION_WRONG);
            }

            if (sInfoOld == null) {
                LOG.warn("handleRemove -> no find value of key: " + new String(key), HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
                return new MdbResult(key, true);
            }

            final String sizeKey = BufferUtil.getSizePeriod(sInfoOld.getLength(), sizeMapping);

            if (StringUtils.isEmpty(sizeKey)) {
                throw new HippoStoreException(String.format("do not find the size type, length is %d", sInfoOld.getLength()), HippoCodeDefine.HIPPO_SIZE_NOT_EXISTED);
            }

            DBAssembleManager manager = dbAssembleMap.get(sizeKey);

            if (manager == null) {
                throw new HippoStoreException("find no DBAssembleManager, size not contain in the size mapping! wanted size is " + sizeKey, HippoCodeDefine.HIPPO_SERVER_ERROR);
            }

            MdbRemoveOper mdbRemoveOper = new MdbRemoveOper(sInfoOld, buck_no, version, new CompleteCallback() {
                @Override
                public void doComplete(MdbPointer info, long modifiedTime) throws HippoException {
                    keyManager.removeStoreInfo(key, buck_no);
                }
            }, key);

            MdbOperResult operResult = manager.handleOper(mdbRemoveOper);

            if (StringUtils.isNotEmpty(operResult.getErrorCode())) {
                throw new HippoStoreException("handleRemove error", operResult.getErrorCode());
            }

            mdbRemoveOper.complete(operResult.getMdbPointer(), operResult.getModifiedTime());

            return new MdbResult(key, true);
        } catch (Exception e) {
            if (e instanceof HippoStoreException) {
                throw (HippoStoreException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new HippoStoreException(e.getMessage(), HIPPO_SERVER_ERROR);
            }
        }
    }

    protected long produceExpireTime(int expire) {
        return (expire > 0 ? System.currentTimeMillis() + expire * 1000 : System.currentTimeMillis() + 30 * 60 * 1000);

    }

    public DBInfo createDirectBufferInfo(int capacity, String blockSize, String dbNo, Integer bucketNo) throws OutOfMaxCapacityException {
        if (validateMemory(capacity)) {
            throw new OutOfMaxCapacityException("Out of max capacity!");
        }

        for (;;) {
            int curt = curPosition.get(), next = curt + 1;
            if (curPosition.compareAndSet(curt, next)) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
                    if (StringUtils.isEmpty(dbNo)) {
                        dbNo = UUID.randomUUID().toString();
                    }
                    return new DBInfo(capacity, blockSize, buffer, dbNo, this, bucketNo, lruFate, sizeMapping);
                } catch (OutOfMemoryError x) {
                    throw new OutOfMaxCapacityException("Out of max capacity by system!");
                }
            }
        }
    }

    @Override
    public void reduceCurrentCapacity(int capacity, Integer bucketNo) {
        AtomicLong bucketCapacity = bucketLimits.get(bucketNo);

        if (bucketCapacity == null) {
            if (bucketLimit != -1) {
                LOG.warn("reduceCurrentCapacity | the bucketNo do not exist in this machine , bucketNo -> " + bucketNo);
            }
        } else {
            synchronized (bucketCapacity) {
                bucketCapacity.getAndAdd(0 - capacity);
                LOG.info("detect release , bucketNo -> " + bucketNo + " capacity -> " + bucketCapacity.get());
            }
        }

        synchronized (this) {
            allCapacity = allCapacity - capacity;
            if (allCapacity < limit) {
                full.compareAndSet(true, false);
            }
        }
    }

    private boolean validateMemory(int capacity) {
        synchronized (this) {
            if (full.get()) {
                return full.get();
            }

            if (allCapacity + capacity > limit) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("----limit --->>" + limit + " ----allCapacity---->" + allCapacity);
                }
                full.set(true);
            } else {
                allCapacity = allCapacity + capacity;
            }

            return full.get();
        }
    }

    public boolean validateBucketLimit(int capacitySize, Integer bucketNo) throws OutOfMaxCapacityException {
        AtomicLong capacity = bucketLimits.get(bucketNo);

        if (capacity == null) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("validateBucketLimit | the bucketNo do not exist in this machine , bucketNo -> " + bucketNo);
            }
            return true;
        }

        synchronized (capacity) {
            if (capacity.get() + capacitySize > eachCapacity) {
                return true;
            } else {
                capacity.getAndAdd(capacitySize);
                return false;
            }
        }
    }

    @Override
    public void resetBuckets(List<BucketInfo> resetBuckets) {
        //make sure the expiring will not do when reset the whole engine
        for (;;) {
            if (expiring.compareAndSet(false, true)) {
                //get the expire lock
                break;
            } else {
                synchronized (expiringMutex) {
                    try {
                        expiringMutex.wait(500);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        try {
            synchronized (resetMutex) {
                keyManager.resetBuckets(resetBuckets);

                LOG.info("resetBuckets | before set free the all capacity ->  " + allCapacity);

                for (BucketInfo newInfo : resetBuckets) {
                    Integer buck = newInfo.getBucketNo();
                    AtomicLong capacity = bucketLimits.putIfAbsent(buck, new AtomicLong(0));
                    if (capacity == null) {
                        LOG.info("resetBuckets | bucketLimits added, bucketNo -> " + buck);
                    }
                }

                for (Entry<String, DBAssembleManager> entry : dbAssembleMap.entrySet()) {
                    DBAssembleManager dbAssembleManager = entry.getValue();
                    dbAssembleManager.resetBuckets(resetBuckets);
                }

                for (BucketInfo info : bucketNos) {
                    Integer buck = info.getBucketNo();
                    boolean isContain = false;
                    for (BucketInfo newInfo : resetBuckets) {
                        if (info.getBucketNo().intValue() == newInfo.getBucketNo().intValue()) {
                            isContain = true;
                            break;
                        }
                    }

                    if (!isContain) {
                        AtomicLong bucketUsedCapacity = bucketLimits.remove(buck);
                        if (bucketUsedCapacity != null) {
                            synchronized (this) {
                                allCapacity -= bucketUsedCapacity.get();
                            }
                        } else {
                            LOG.warn("resetBuckets | buck -> " + buck + " capacity not existed!!");
                        }
                    }
                }

                LOG.info("resetBuckets | after set free the all capacity ->  " + allCapacity);

                bucketNos.clear();
                bucketNos.addAll(resetBuckets);
                expiring.set(false);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            LOG.info("MdbManagerImpl | resetBuckets is finished!");
        }
    }

    public int getBuckCount() {
        if (bucketNos != null) {
            return bucketNos.size();
        }
        return 0;
    }

    public List<BucketInfo> getBucketNos() {
        return bucketNos;
    }

    public void setCapacityController(CapacityController capacityController) {
        this.capacityController = capacityController;
    }

    public int countDB() {
        int count = 0;
        for (DBAssembleManager dbAssembleManager : dbAssembleMap.values()) {
            count = count + dbAssembleManager.countDB();
        }
        return count;
    }

    @Override
    public void doExpireAndGetDeletedDBIfs() {
        long beginTime = System.currentTimeMillis();
        if (expiring.compareAndSet(false, true)) {
            try {
                for (String sizeFlag : dbAssembleMap.keySet()) {
                    //某个桶号下的过期dbinfos
                    DBAssembleManager dbAssembleManager = dbAssembleMap.get(sizeFlag);

                    if (dbAssembleManager != null) {
                        Map<Integer, List<String>> disposeDBInfos = dbAssembleManager.expire();

                        if (disposeDBInfos == null) {
                            LOG.info("MdbManagerImpl detect the end flag!!");
                            return;
                        }

                        if (disposeDBInfos.size() > 0) {
                            for (Integer bucketNo : disposeDBInfos.keySet()) {
                                List<String> deleteDBInfos = disposeDBInfos.get(bucketNo);
                                if (deleteDBInfos != null && deleteDBInfos.size() > 0) {
                                    if (callbacks != null) {
                                        for (TransDataCallBack callBack : callbacks) {
                                            callBack.updateExpiredCallBack(bucketNo, System.currentTimeMillis());
                                        }
                                    }
                                    deleteDBInfos.clear();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("unexpected error when doing expire and LRU!", e);
            } finally {
                expiring.set(false);
                long costTime = System.currentTimeMillis() - beginTime;
                LOG.info(String.format("period expire and LRU prepare is end! cost time is %d ms !!", costTime));
            }
        }
    }

    private Serializer createSerializer(String type) {
        Serializer serializer = null;
        lock.lock();
        try {
            serializer = serializerMap.get(type);
            if (serializer == null) {
                throw new RuntimeException("can't find the sender impl class of type[" + type + "]");
            }
        } finally {
            lock.unlock();
        }
        return serializer;
    }

    @Override
    public byte[] getSeparator() throws IOException {
        return separator;
    }

    @Override
    public int getSize() {
        return keyManager.getSize();
    }

    @Override
    public long getCurrentUsedCapacity() {
        return allCapacity;
    }

    @Override
    public byte[] duplicateDirectBuffer(Integer bucketNo, String sizeFlag, String dbIfId, int offset, int size) {
        byte[] duplicateData = null;
        DBAssembleManager dbassembleManager = dbAssembleMap.get(sizeFlag);
        if (dbassembleManager != null) {
            duplicateData = dbassembleManager.duplicateDirectBuffer(bucketNo, dbIfId, offset, size);
        } else {
            LOG.warn(String.format("dbassemblemap for the size flag [%s] could not be found!!", sizeFlag));
        }
        return duplicateData;
    }

    @Override
    public Map<String, List<String>> collectDBIfs(Integer buckId) {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        synchronized (dbAssembleMap) {
            for (Entry<String, DBAssembleManager> entry : dbAssembleMap.entrySet()) {
                DBAssembleManager assembleManager = entry.getValue();
                String sizeFlag = entry.getKey();
                List<String> list = assembleManager.getDBIfIdsByBucketNo(buckId);
                result.put(sizeFlag, list);
            }
        }
        return result;
    }

    @Override
    public boolean syncDBIf(Integer bucketNo, String sizeFlag, String dbInfoId, int offset, byte[] data) throws HippoStoreException {
        DBAssembleManager manager = dbAssembleMap.get(sizeFlag);
        return manager.syncDBIf(bucketNo, dbInfoId, offset, data);
    }

    @Override
    public void deleteDBIf(int bucketNo, String sizeFlag, String dbIfId) {
        DBAssembleManager dbAssembleManager = dbAssembleMap.get(sizeFlag);
        if (dbAssembleManager != null) {
            dbAssembleManager.deleteDBIf(bucketNo, dbIfId);
        }
    }

    public List<TransDataCallBack> getDataTransCallBackList() {
        return callbacks;
    }

    public void setDataTransCallBackList(List<TransDataCallBack> callbacks) {
        this.callbacks = callbacks;
    }

    @Override
    public Map<String, Set<String>> collectDBIfsBetweenGivenTime(Integer bucketNo, long beginTime, long endTime) {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();
        for (String sizeFlag : dbAssembleMap.keySet()) {
            DBAssembleManager dbassembleManager = dbAssembleMap.get(sizeFlag);
            Set<String> dbinfos = dbassembleManager.collectDBIfsBetweenGivenTime(bucketNo, beginTime, endTime);
            if (dbinfos != null && dbinfos.size() > 0) {
                result.put(sizeFlag, dbinfos);
            }
        }
        return result;
    }

    @Override
    public DBInfo fetchNewDBInfo(int capacitySize, String sizeModel, String createDbId, Integer bucketNo) throws OutOfMaxCapacityException {
        DBInfo info;

        if (usingBucketLimit && validateBucketLimit(capacitySize, bucketNo)) {
            StringBuilder errMsg = new StringBuilder("for bucketNo " + bucketNo + " Out of max capacity!");

            if (bucketLimits.get(bucketNo) != null) {
                errMsg.append(" current capacity -> ").append(bucketLimits.get(bucketNo));
            }

            throw new OutOfMaxCapacityException(errMsg.toString());
        }

        if (capacityController != null) {
            info = capacityController.getNewDbInfo(capacitySize, sizeModel, createDbId, bucketNo);

            if (info != null) {
                if (StringUtils.isNotEmpty(createDbId)) {
                    info.setDbNo(createDbId);
                }
                return info;
            }
        }

        try {
            info = createDirectBufferInfo(capacitySize, sizeModel, createDbId, bucketNo);
        } catch (OutOfMaxCapacityException e) {
            if (usingBucketLimit) {
                reduceCurrentCapacity(capacitySize, bucketNo);
            }
            throw e;
        }

        if (info != null) {
            LOG.info("could not get from the pool, need to create new dbInfoId directly! create dbIfo id -> " + info.getDbNo());
            if (capacityController != null) {
                capacityController.notifyWait();
            }
        }
        return info;
    }

    public void expireOffsetCallBack(Integer bucketNo, long modifyTime) {
        if (callbacks != null) {
            for (TransDataCallBack callBack : callbacks) {
                callBack.updateModifiedTimeCallBack(bucketNo, modifyTime);
            }
        }
    }

    @Override
    public long getBucketLatestModifiedTime(Integer bucketNo, boolean useDefault) {
        long result = 0;
        long currentTime = System.currentTimeMillis();
        for (DBAssembleManager dbAssembleManager : dbAssembleMap.values()) {
            long time = dbAssembleManager.getBucketLatestModifiedTime(bucketNo);
            result = Math.max(result, time);
        }
        if (result == 0) {
            if (useDefault) {
                return currentTime;
            } else {
                return result;
            }
        } else {
            return result;
        }
    }

    @Override
    public void expireOffsetTime(DBInfo dbIf, int offset) {
        DBAssembleManager dbassembleManager = dbAssembleMap.get(dbIf.getSizeModel());
        if (dbassembleManager != null) {
            dbassembleManager.expireOffset(dbIf, offset, false);
        }
    }

    @Override
    public void expireOffsetTimeWhenSyncData(MdbPointer point, byte[] key) throws HippoStoreException {
        if (point != null) {
            String sizeType = getSizeType(point.getLength());
            if (StringUtils.isNotEmpty(sizeType)) {
                DBAssembleManager manager = dbAssembleMap.get(sizeType);
                MdbRemoveOper oper = new MdbRemoveOper(point, point.getBuckId(), MdbConstants.DEFAULT_VERSION, null, key);
                MdbOperResult result = manager.handleOper(oper);
                if (StringUtils.isNotEmpty(result.getErrorCode())) {
                    throw new HippoStoreException("expireOffsetTimeWhenSyncData when sync data!", result.getErrorCode());
                }
            } else {
                LOG.warn("expireOffsetTimeWhenSyncData | sizeType is null , dbInfoId -> " + point.getDbNo() + " , bucketNo -> " + point.getBuckId() + ", sizeType -> " + sizeType);
            }
        } else {
            LOG.warn("expireOffsetTimeWhenSyncData | point is null");
        }
    }

    @Override
    public void deleteByKey(byte[] key, DBInfo dbIfo, int offset) {
        boolean removeFlag = false;
        if (key.length > 0) {
            MdbPointer oldPoint = keyManager.getStoreInfo(key, dbIfo.getBucketNo());
            if (oldPoint != null) {
                DBAssembleManager dbassembleManager;
                synchronized (oldPoint) {
                    String sizeKey = BufferUtil.getSizePeriod(oldPoint.getBuckId(), sizeMapping);
                    dbassembleManager = dbAssembleMap.get(sizeKey);
                    if (dbassembleManager != null && dbIfo.getDbNo().equals(oldPoint.getDbNo()) && oldPoint.getOffset() == offset) {
                        keyManager.removeStoreInfo(key, dbIfo.getBucketNo());
                        removeFlag = true;
                    }
                }
                if (removeFlag) {
                    dbassembleManager.expireOffset(dbIfo, offset, true);
                }
            }
        }

    }

    @Override
    public void removeKeyFromMap(byte[] key, DBInfo dbIf, int offset) {
        MdbPointer oldPoint = keyManager.getStoreInfo(key, dbIf.getBucketNo());
        if (oldPoint != null) {
            synchronized (oldPoint) {
                if (dbIf.getDbNo().equals(oldPoint.getDbNo()) && oldPoint.getOffset() == offset) {
                    keyManager.removeStoreInfo(key, dbIf.getBucketNo());
                }
            }
        }
    }

    @Override
    public long getBucketUsedCapacity(Integer bucketNo) {
        AtomicLong bucketLimit = bucketLimits.get(bucketNo);
        if (bucketLimit != null) {
            return bucketLimit.get();
        } else {
            return 0;
        }

    }

    @Override
    public String getSizeType(int length) {
        return BufferUtil.getSizePeriod(length, sizeMapping);
    }

    @Override
    public List<SyncDataTask> getTasksNotExistedInSyncList(Set<String> dbIfs, Integer bucketNo, String sizeFlag) throws HippoStoreException {
        DBAssembleManager manager = dbAssembleMap.get(sizeFlag);
        if (manager != null) {
            return manager.getTasksNotInSyncList(dbIfs, bucketNo);
        } else {
            LOG.warn("size flag -> " + sizeFlag + " not existed!");
            return null;
        }
    }

    @Override
    public boolean verifyExpiredDbInfo(Integer bucketNo, String sizeFlag, String dbIfId) {
        DBAssembleManager manager = dbAssembleMap.get(sizeFlag);
        boolean isContain;
        if (manager != null) {
            isContain = manager.verifyExpiredDbIf(dbIfId, bucketNo);
        } else {
            LOG.warn("size flag -> " + sizeFlag + " not existed! when do verifyExpiredDbIfs");
            isContain = false;
        }
        return isContain;
    }

    @Override
    public LinkedList<SyncDataTask> emergencyVerifyBucket(Integer bucketNo) throws HippoStoreException {
        LinkedList<SyncDataTask> result = new LinkedList<SyncDataTask>();
        int size = 0;
        for (DBAssembleManager dbAssembleManager : dbAssembleMap.values()) {
            List<SyncDataTask> subResult = dbAssembleManager.getTasksNotInSyncList(null, bucketNo);
            if (subResult != null) {
                result.addAll(subResult);
                size += subResult.size();
                subResult.clear();
            }
        }

        //capacity check
        long usedCapacity = (long) size * (long) MdbConstants.CAPACITY_SIZE;

        AtomicLong bucketCapacity = bucketLimits.get(bucketNo);

        LOG.info("begin to check capacity for bucketNo-> " + bucketNo);

        if (bucketCapacity != null) {
            long bet = 0;
            long oldSize = 0;
            synchronized (bucketCapacity) {
                if (bucketCapacity.get() != usedCapacity) {
                    //reset the used size
                    oldSize = bucketCapacity.get();
                    bet = bucketCapacity.get() - usedCapacity;
                    bucketCapacity.set(usedCapacity);
                }
            }

            synchronized (this) {
                if (bet != 0) {
                    LOG.warn("before detect the capacity not the same, bucketNo-> " + bucketNo + " , old capacity -> " + oldSize + ", dbIfo number -> " + size + ", allCapacity -> " + allCapacity);
                }

                allCapacity = allCapacity - bet;

                if (bet != 0) {
                    LOG.warn("after detect the capacity not the same, bucketNo-> " + bucketNo + " , new capacity -> " + usedCapacity + ", dbIfo number -> " + size + ", allCapacity -> " + allCapacity);
                }

            }
        } else {
            LOG.error("emergencyVerifyBucket could not found the bucketNo -> " + bucketNo + "  capacity!!");
        }

        LOG.info("end of check capacity for bucketNo-> " + bucketNo);

        return result;
    }

    @Override
    public double getUsedPercent(Integer bucketNo) throws HippoStoreException {
        AtomicLong bucketCapacity = bucketLimits.get(bucketNo);
        if (bucketCapacity == null) {
            throw new HippoStoreException("bucketNo -> " + bucketNo + " not existed!!", HIPPO_BUCKET_NOT_EXISTED);
        }
        return bucketCapacity.doubleValue() / eachCapacity;
    }

    @Override
    public List<SyncDataTask> getWholeDBIfs(Integer bucketNo) throws HippoStoreException {
        List<SyncDataTask> result = new ArrayList<SyncDataTask>();
        for (Entry<String, DBAssembleManager> entry : dbAssembleMap.entrySet()) {
            String sizeFlag = entry.getKey();
            DBAssembleManager manager = entry.getValue();
            if (manager != null) {
                List<SyncDataTask> subResult = manager.getTasksNotInSyncList(null, bucketNo);
                result.addAll(subResult);
                subResult.clear();
            } else {
                throw new HippoStoreException("size flag -> " + sizeFlag + " not existed!", HIPPO_SIZE_NOT_EXISTED);
            }
        }
        return result;
    }

    @Override
    public boolean memoryCheckFull(Integer bucketNo) {
        if (usingBucketLimit) {
            boolean bucketLimitFull;
            AtomicLong capacity = bucketLimits.get(bucketNo);
            if (capacity == null) {
                LOG.warn("validateBucketLimit | the bucketNo do not exist in this machine , bucketNo -> " + bucketNo);
                bucketLimitFull = true;
            } else {
                synchronized (capacity) {
                    if (capacity.get() + MdbConstants.CAPACITY_SIZE > eachCapacity) {
                        bucketLimitFull = true;
                    } else {
                        bucketLimitFull = false;
                    }
                }
            }
            return bucketLimitFull;
        } else {
            return full.get();
        }
    }

    private Object getCounterMutex(byte[] data) {
        int hash = HashUtil.murmurhash2(data, MUTEX_ARRAY_SIZE);
        int index = Math.abs(hash) % MUTEX_ARRAY_SIZE;
        return counterMutex[index];
    }
}
