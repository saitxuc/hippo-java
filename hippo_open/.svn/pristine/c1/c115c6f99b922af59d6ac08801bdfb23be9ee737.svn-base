package com.pinganfu.hippo.mdb.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.common.SyncDataTask;
import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.common.util.ByteUtil;
import com.pinganfu.hippo.common.util.Logarithm;
import com.pinganfu.hippo.mdb.BlockSizeMapping;
import com.pinganfu.hippo.mdb.DBAssembleManager;
import com.pinganfu.hippo.mdb.KeyManager;
import com.pinganfu.hippo.mdb.MdbConstants;
import com.pinganfu.hippo.mdb.MdbManager;
import com.pinganfu.hippo.mdb.MdbOper;
import com.pinganfu.hippo.mdb.exception.OutOfMaxCapacityException;
import com.pinganfu.hippo.mdb.obj.DBAssembleInfo;
import com.pinganfu.hippo.mdb.obj.DBInfo;
import com.pinganfu.hippo.mdb.obj.FetchBean;
import com.pinganfu.hippo.mdb.obj.FetchBitBean;
import com.pinganfu.hippo.mdb.obj.MdbAddOper;
import com.pinganfu.hippo.mdb.obj.MdbBitGetOper;
import com.pinganfu.hippo.mdb.obj.MdbBitUpdateOper;
import com.pinganfu.hippo.mdb.obj.MdbGetOper;
import com.pinganfu.hippo.mdb.obj.MdbOperResult;
import com.pinganfu.hippo.mdb.obj.MdbPointer;
import com.pinganfu.hippo.mdb.obj.MdbRemoveOper;
import com.pinganfu.hippo.mdb.obj.MdbUpdateOper;
import com.pinganfu.hippo.mdb.obj.OffsetInfo;
import com.pinganfu.hippo.mdb.utils.BufferUtil;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * @author saitxuc write 2014-7-29
 * @author Owen
 */
public class DBAssembleManagerImpl extends LifeCycleSupport implements DBAssembleManager {

    protected static final Logger LOG = LoggerFactory.getLogger(DBAssembleManagerImpl.class);

    private final ConcurrentHashMap<Integer, DBAssembleInfo> assembleMap = new ConcurrentHashMap<Integer, DBAssembleInfo>();
    private final List<BucketInfo> bucketNos = new ArrayList<BucketInfo>();
    private String sizeModel;
    private MdbManager mdbManager;

    private KeyManager keyManager;

    private BlockSizeMapping sizeMapping;

    private int expireLimit = 10;

    public DBAssembleManagerImpl(List<BucketInfo> bucketNos, String sizeModel, MdbManager mdbManager, KeyManager keyManager, BlockSizeMapping sizeMapping,
                                 int expireLimit) {
        this.sizeModel = sizeModel;
        this.bucketNos.addAll(bucketNos);
        this.mdbManager = mdbManager;
        this.keyManager = keyManager;
        this.sizeMapping = sizeMapping;
        this.expireLimit = expireLimit;
        init();
    }

    public MdbOperResult handleOper(MdbOper oper) {
        switch (oper.getOper()) {
            case ADD_OPER:
                return add((MdbAddOper) oper);
            case GET_OPER:
                return get((MdbGetOper) oper);
            case UPDATE_OPER:
                return update((MdbUpdateOper) oper);
            case REMOVE_OPER:
                return remove((MdbRemoveOper) oper);
            case BITUPDATE_OPER:
                return bitUpdate((MdbBitUpdateOper) oper);
            case BITGET_OPER:
                return bitGet((MdbBitGetOper) oper);
            default:
                return null;
        }
    }
    
    private MdbOperResult bitGet(MdbBitGetOper oper) {
        MdbOperResult oresult = new MdbOperResult();
        try {
            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("this bucket %d not existed!!", oper.getBuckNo()));
                oresult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return oresult;
            }

            DBInfo dbInfo = info.getDbInfoMap().get(oper.getsInfo().getDbNo());

            FetchBitBean fetchBean = dbInfo.fetchBitMemory(oper.getsInfo().getOffset(), oper.getByteIndex(), mdbManager.getSeparator().length);

            byte[] realKey = fetchBean.getKey();

            boolean isContain = ByteUtil.isSame(realKey, oper.getKey());

            if (isContain) {
                boolean bitResult = (fetchBean.getOrginData() & (1 << (7 - oper.getOffsetInByte()))) != 0;
                long expireTime = Logarithm.getLong(fetchBean.getExpireTime(), 0);
                if (expireTime < System.currentTimeMillis()) {
                    oresult.setErrorCode(HippoCodeDefine.HIPPO_DATA_EXPIRED);
                } else {
                    oresult.setContent(ByteUtil.parseBoolean(bitResult));
                    oresult.setVersion(fetchBean.getVersion());
                    oresult.setExpireTime(expireTime);
                }
            } else {
                oresult.setErrorCode(HippoCodeDefine.HIPPO_DATA_EXPIRED);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            oresult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
        }
        return oresult;
    }

    private MdbOperResult bitUpdate(MdbBitUpdateOper oper) {
        MdbOperResult oresult = new MdbOperResult();
        try {
            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("DBAssembleManagerImpl | this bucket %d not existed!!", oper.getBuckNo()));
                oresult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return oresult;
            }

            DBInfo dbInfo = info.getDbInfoMap().get(oper.getInfo().getDbNo());

            ByteBuffer buffer = dbInfo.getByteBuffer();

            final int position = oper.getInfo().getOffset();

            int offsetInBlock = oper.getOffset() % oper.getBitSizeLeft();
            final int byteIndex = offsetInBlock >> 3;
            final int offsetInByte = offsetInBlock & 0x7;

            int offsetInBuffer = position + oper.getKey().length + 12 + oper.getSeparatorLength() + byteIndex;
            //**begin
            synchronized (buffer) {
                if (dbInfo.getIsDispose()) {
                    LOG.error(String.format("DBAssembleManagerImpl | update | this bucket %d ,dbinfo -> %s has been disposed", oper.getBuckNo(), dbInfo
                            .getDbNo()));
                    oresult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                    return oresult;
                }

                buffer.position(position);

                int keyLength = buffer.getInt();

                int oldVersion = buffer.getInt();

                int oldContentLength = buffer.getInt();

                byte[] existedKey = new byte[keyLength];

                buffer.get(existedKey, 0, keyLength);

                buffer.position(position + oldContentLength + 4);

                long expireTime = buffer.getLong();

                if (!ByteUtil.isSame(existedKey, oper.getKey()) || expireTime == -1) {
                    oresult.setErrorCode(HippoCodeDefine.HIPPO_DATA_EXPIRED);
                    return oresult;
                }

                if (System.currentTimeMillis() > expireTime) {
                    byte[] resetData = new byte[oper.getBitSizeLeft() / 8 - keyLength - oper.getSeparatorLength() * 2 - 8];
                    buffer.position(position + 12 + keyLength + oper.getSeparatorLength());
                    buffer.put(resetData, 0, resetData.length);
                }

                buffer.position(position + MdbConstants.HEADER_LENGTH_FOR_INT);

                if (oper.getVersion() == 0) {
                    buffer.putInt(oldVersion);
                    oper.getInfo().setVersion(oldVersion);
                } else if (oldVersion == oper.getVersion()) {
                    buffer.putInt(oldVersion + 1);
                    oper.getInfo().setVersion(oldVersion + 1);
                } else {
                    oresult.setErrorCode(HippoCodeDefine.HIPPO_OPERATION_VERSION_WRONG);
                    return oresult;
                }

                byte tempdata = buffer.get(offsetInBuffer);
                if (oper.isVal()) {
                    tempdata |= (1 << (7 - offsetInByte));
                } else {
                    tempdata &= ~(1 << (7 - offsetInByte));
                }
                buffer.position(offsetInBuffer);
                buffer.put(tempdata);

                buffer.position(position + oldContentLength + 4);
                buffer.put(oper.getExpire());
            }

            //update the modify time and used to sync data
            dbInfo.updateModifyTime();

            oresult.setMdbPointer(oper.getInfo());

            oresult.setModifiedTime(dbInfo.getModifyTime());

            return oresult;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            oresult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
        }
        return oresult;
    }

    public boolean verifyKey(byte[] key, MdbPointer pointer) {
        try {
            DBAssembleInfo info = assembleMap.get(pointer.getBuckId());
            DBInfo dbInfo = info.getDbInfoMap().get(pointer.getDbNo());
            if (dbInfo == null) {
                return false;
            }
            FetchBean fetchBean = dbInfo.fetchMemory(pointer.getOffset(), pointer.getLength());
            byte[] originalData = fetchBean.getOriginData();
            return ByteUtil.isSame(key, BufferUtil.separateKey(originalData, MdbConstants.HEADER_LENGTH_FOR_INT * 3, fetchBean.getKLength()));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return false;
        }
    }

    private MdbOperResult add(MdbAddOper oper) {
        MdbOperResult oResult = new MdbOperResult();
        try {
            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("this bucket %d not existed!!", oper.getBuckNo()));
                oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return oResult;
            }

            OffsetInfo offsetInfo = info.getCurrentDBOffset();

            if (offsetInfo == null) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(String.format("this bucket %d has been removed!!", oper.getBuckNo()));
                }
                oResult.setErrorCode(HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED);
            } else {
                DBInfo dbInfo = info.getDbInfoMap().get(offsetInfo.getDbNo());

                if (offsetInfo.isLru()) {
                    processLRUOffSet(offsetInfo, dbInfo);
                }

                ByteBuffer buffer = offsetInfo.getBuffer();
                int finalVersion;
                synchronized (buffer) {
                    if (dbInfo.getIsDispose()) {
                        LOG.error(String.format("DBAssembleManagerImpl | add | this bucket %d ,dbInfo -> %s has been disposed", oper.getBuckNo(), dbInfo
                                .getDbNo()));
                        oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                        return oResult;
                    }

                    buffer.position(offsetInfo.getOffset());
                    buffer.putInt(oper.getkLength());
                    if (oper.getVersion() == 0) {
                        buffer.putInt(1);
                        finalVersion = 1;
                    } else {
                        buffer.putInt(oper.getVersion());
                        finalVersion = oper.getVersion();
                    }
                    buffer.putInt(oper.getContent().length);
                    buffer.put(oper.getContent(), 0, oper.getContent().length);
                }

                offsetInfo.setExpireTime(oper.getExpireTime());
                offsetInfo.setKLength(oper.getkLength());
                dbInfo.updateModifyTime();

                MdbPointer pointer = new MdbPointer(oper.getBuckNo(), offsetInfo.getDbNo(), offsetInfo.getOffset(), (oper.getContent().length + MdbConstants.HEADER_LENGTH_FOR_INT * 3), finalVersion);
                oResult.setMdbPointer(pointer);
                oResult.setModifiedTime(dbInfo.getModifyTime());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            if (e instanceof HippoStoreException) {
                oResult.setErrorCode(((HippoStoreException) e).getErrorCode());
            } else {
                oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
            }
        }
        return oResult;
    }

    private void processLRUOffSet(OffsetInfo offsetInfo, DBInfo dbInfo) {
        try {
            if (offsetInfo.setRecycle(true, false)) {
                //has recycle, do not need remove the key from the map
                offsetInfo.reset();
            } else {
                if (offsetInfo.getExpireTime() != -1) {
                    //get an used offset
                    FetchBean fetchBean = dbInfo.fetchMemory(offsetInfo.getOffset() + MdbConstants.HEADER_LENGTH_FOR_INT * 3, offsetInfo.getKLength());
                    byte[] keyData = BufferUtil.separateKey(fetchBean.getOriginData(), 0, offsetInfo.getKLength());
                    keyManager.removeStoreInfo(keyData, dbInfo.getBucketNo());
                    offsetInfo.setRecycle(true, false);
                } else {
                    //get an free offset
                    dbInfo.getFreeCount().decrementAndGet();
                    offsetInfo.setRecycle(true, false);
                }
            }
        } finally {
            offsetInfo.setLru(false);
        }
    }

    private MdbOperResult get(MdbGetOper oper) {
        MdbOperResult oResult = new MdbOperResult();
        try {
            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("this bucket %d not existed!!", oper.getBuckNo()));
                oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return oResult;
            }

            DBInfo dbInfo = info.getDbInfoMap().get(oper.getsInfo().getDbNo());

            FetchBean fetchBean = dbInfo.fetchMemory(oper.getsInfo().getOffset(), oper.getsInfo().getLength());

            byte[] originalData = fetchBean.getOriginData();
            byte[] realKey = BufferUtil.separateKey(originalData, MdbConstants.HEADER_LENGTH_FOR_INT * 3, fetchBean.getKLength());
            int version = Logarithm.bytesToInt(originalData, 4);

            boolean isContain = ByteUtil.isSame(realKey, oper.getGkey());

            if (isContain) {
                int separatorLength = mdbManager.getSeparator().length;
                byte[] data = BufferUtil.separate(originalData, fetchBean.getKLength(), separatorLength, MdbConstants.LONG_BYTE_SIZE);
                long expireTime = Logarithm.getLong(originalData, oper.getsInfo().getLength() - 8);
                oResult.setContent(data);
                oResult.setVersion(version);
                oResult.setExpireTime(expireTime);
            } else {
                oResult.setErrorCode(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
        }
        return oResult;
    }

    protected MdbOperResult update(MdbUpdateOper oper) {
        MdbOperResult oResult = new MdbOperResult();
        try {
            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("DBAssembleManagerImpl | this bucket %d not existed!!", oper.getBuckNo()));
                oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return oResult;
            }

            DBInfo dbInfo = info.getDbInfoMap().get(oper.getInfo().getDbNo());

            ByteBuffer bufferRef = dbInfo.getByteBuffer();

            final int position = oper.getInfo().getOffset();

            synchronized (bufferRef) {
                if (dbInfo.getIsDispose()) {
                    LOG.error(String.format("DBAssembleManagerImpl | update | this bucket %d ,dbInfo -> %s has been disposed", oper.getBuckNo(), dbInfo
                            .getDbNo()));
                    oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                    return oResult;
                }

                bufferRef.position(position);

                int keyLength = bufferRef.getInt();

                int oldVersion = bufferRef.getInt();

                int oldContentLength = bufferRef.getInt();

                byte[] existedKey = new byte[keyLength];

                bufferRef.get(existedKey, 0, keyLength);

                bufferRef.position(position + oldContentLength + 4);

                long expireTime = bufferRef.getLong();

                if (expireTime == -1 || System.currentTimeMillis() > expireTime) {
                    oResult.setErrorCode(HippoCodeDefine.HIPPO_DATA_EXPIRED);
                    return oResult;
                }

                byte[] compareKey = new byte[keyLength];

                System.arraycopy(oper.getContent(), 0, compareKey, 0, keyLength);

                if (!ByteUtil.isSame(existedKey, compareKey)) {
                    oResult.setErrorCode(HippoCodeDefine.HIPPO_DATA_EXPIRED);
                    return oResult;
                }

                bufferRef.position(position + MdbConstants.HEADER_LENGTH_FOR_INT);

                if (oper.getVersion() == 0) {
                    bufferRef.putInt(oldVersion);
                    oper.getInfo().setVersion(oldVersion);
                } else if (oldVersion == oper.getVersion()) {
                    bufferRef.putInt(oldVersion + 1);
                    oper.getInfo().setVersion(oldVersion + 1);
                } else {
                    oResult.setErrorCode(HippoCodeDefine.HIPPO_OPERATION_VERSION_WRONG);
                    return oResult;
                }

                bufferRef.putInt(oper.getContent().length);

                bufferRef.put(oper.getContent(), 0, oper.getContent().length);
            }

            //update the modify time and used to sync data
            dbInfo.updateModifyTime();

            oResult.setMdbPointer(oper.getInfo());

            oResult.setModifiedTime(dbInfo.getModifyTime());

            return oResult;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            oResult.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
        }
        return oResult;
    }

    public MdbOperResult remove(MdbRemoveOper oper) {
        MdbOperResult result = new MdbOperResult();
        try {

            DBAssembleInfo info = assembleMap.get(oper.getBuckNo());

            if (info == null) {
                LOG.error(String.format("this bucket %d not existed!!", oper.getBuckNo()));
                result.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
                return result;
            }

            DBInfo dbInfo = info.getDbInfoMap().get(oper.getInfo().getDbNo());

            if (dbInfo == null) {
                LOG.error("dbInfo not existed! dbInfo -> " + oper.getInfo().getDbNo() + " , bucket -> " + oper.getInfo().getBuckId());
                result.setErrorCode(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
            } else {
                FetchBean fetchBean = dbInfo.fetchMemory(oper.getInfo().getOffset() + MdbConstants.HEADER_LENGTH_FOR_INT, oper.getInfo().getKLength() + 8);

                byte[] originalData = fetchBean.getOriginData();
                int version = Logarithm.bytesToInt(originalData, 0);
                byte[] kData = BufferUtil.separateKey(originalData, 8, oper.getInfo().getKLength());

                //verify the version with the version in the buffer
                if (oper.getVersion() == 0 || oper.getVersion() == version) {
                    if (ByteUtil.isSame(kData, oper.getKey())) {
                        result.setMdbPointer(oper.getInfo());
                        result.setRemoveKey(kData);
                        OffsetInfo oinfo = dbInfo.findOffsetInfoByOffset(oper.getInfo().getOffset());
                        //set the flag,this block could be recycle
                        dbInfo.modifyExpireTimeInBuffer(oinfo, Logarithm.putLong(-1L));
                        oinfo.setRecycle(false, true);
                    } else {
                        result.setErrorCode(HippoCodeDefine.HIPPO_DATA_DOES_NOT_EXIST);
                    }
                } else {
                    result.setErrorCode(HippoCodeDefine.HIPPO_OPERATION_VERSION_WRONG);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setErrorCode(HippoCodeDefine.HIPPO_SERVER_ERROR);
        }
        return result;
    }

    @Override
    public void doInit() {
        for (BucketInfo info : bucketNos) {
            DBAssembleInfo assembleInfo = assembleMap
                    .putIfAbsent(info.getBucketNo(), new DBAssembleInfo(sizeModel, mdbManager, info.getBucketNo(), sizeMapping, expireLimit));
            if (assembleInfo == null) {
                assembleMap.get(info.getBucketNo()).initCreateDefaultDBInfo();
            }
        }
    }

    @Override
    public void doStart() {

    }

    @Override
    public void doStop() {
        try {
            synchronized (assembleMap) {
                for (DBAssembleInfo dbAssembleInfo : assembleMap.values()) {
                    dbAssembleInfo.dispose();
                }
                assembleMap.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
        }

    }

    public int countDB() {
        int count = 0;
        for (DBAssembleInfo dbAssembleInfo : assembleMap.values()) {
            count = count + dbAssembleInfo.getDbInfoMap().size();
        }
        return count;
    }

    @Override
    public Map<Integer, List<String>> expire() {
        //prepare and get all bucketNo
        Set<Integer> bucketsTask = new HashSet<Integer>();
        if (assembleMap != null) {
            bucketsTask.clear();
            synchronized (bucketNos) {
                for (BucketInfo info : bucketNos) {
                    if (!info.isSlave()) {
                        bucketsTask.add(info.getBucketNo());
                    }
                }
            }
        }

        //doing iterator the map could not modified
        Map<Integer, List<String>> wholeDisIfs = new HashMap<Integer, List<String>>();
        for (Integer buckNo : bucketsTask) {
            DBAssembleInfo dbAssembleInfo = assembleMap.get(buckNo);
            if (dbAssembleInfo != null) {
                //LOG.dbInfo("begin to do bucket -> " + buckNo + " expire!");
                List<String> disIfs = dbAssembleInfo.expire();

                if (disIfs == null) {
                    LOG.info("DBAssembleManagerImpl detect stop flag");
                    return null;
                }

                if (disIfs.size() > 0) {
                    wholeDisIfs.put(buckNo, disIfs);
                }
            }
        }
        return wholeDisIfs;
    }

    @Override
    public int getSize() {
        return keyManager.getSize();
    }

    @Override
    public byte[] duplicateDirectBuffer(Integer bucketNo, String dbIfId, int offset, int size) {
        DBAssembleInfo dbAssembleInfo = assembleMap.get(bucketNo);
        return dbAssembleInfo.duplicateDirectBuffer(dbIfId, offset, size);
    }

    @Override
    public List<String> getDBIfIdsByBucketNo(Integer bucketNo) {
        List<String> dbIfs = new ArrayList<String>();
        DBAssembleInfo assembleInfo = assembleMap.get(bucketNo);
        if (assembleInfo != null) {
            dbIfs.addAll(assembleInfo.getDbInfoMap().keySet());
        }
        return dbIfs;
    }

    @Override
    public boolean syncDBIf(Integer bucketNo, String dbIfId, int offset, byte[] data) throws HippoStoreException {
        DBAssembleInfo assembleInfo = assembleMap.get(bucketNo);

        if (assembleInfo == null) {
            throw new HippoStoreException(String.format("DBAssembleManagerImpl | bucket not existed when sync dbInfo , bucket number is %d", bucketNo), HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED);
        }

        int sizePer = sizeMapping.getSIZE_PER(sizeModel);
        int count = sizeMapping.getSIZE_COUNT(sizeModel);

        DBInfo dbInfo = null;
        try {
            dbInfo = assembleInfo.findOrCreateDBInfo(dbIfId, bucketNo);

            if (!dbInfo.startUsing()) {
                LOG.warn(String.format("dbInfo %s is in deleting !!", dbInfo.getDbNo()));
            }

            byte[] dbInfoModifiedTime = new byte[8];

            // copy the block modifyTime
            // the last 8 byte is the modify time
            System.arraycopy(data, data.length - 8, dbInfoModifiedTime, 0, 8);

            dbInfo.setModifiedTime(Logarithm.getLong(dbInfoModifiedTime, 0));

            for (int index = 0; index < count; index++) {
                if (dbInfo.getSyncDispose()) {
                    //detect the dispose flag
                    if (LOG.isWarnEnabled()) {
                        LOG.warn(String.format("dbInfo %s was set dispose when in sync!!", dbInfo.getDbNo()));
                    }
                    break;
                }

                int offsetInByte = index * sizePer;
                int newKeyLength = Logarithm.bytesToInt(data, offsetInByte);

                if (newKeyLength > 0) {
                    OffsetInfo oInfo = dbInfo.findOffsetInfoByOffset(offsetInByte);

                    if (oInfo == null) {
                        LOG.error(String
                                .format("DBAssembleManagerImpl | the data when doing replication could not find the offset, dbInfo %s, offset %d ,bucket %d!!", dbIfId, offsetInByte, bucketNo));
                        return false;
                    }

                    int newVersion = Logarithm.bytesToInt(data, offsetInByte + 4);
                    int newContentLength = Logarithm.bytesToInt(data, offsetInByte + 8);
                    byte newKey[] = Arrays.copyOfRange(data, offsetInByte + 12, offsetInByte + 12 + newKeyLength);
                    long newExpireTime = Logarithm.getLong(data, offsetInByte + newContentLength + 4);
                    byte[] newByteBuffer = Arrays.copyOfRange(data, offsetInByte, offsetInByte + sizePer);

                    long contentLength;
                    synchronized (oInfo.getBuffer()) {
                        if (dbInfo.getIsDispose()) {
                            LOG.error(String.format("syncDBIf | this bucket -> %d ,dbInfo -> %s has been disposed", dbInfo.getBucketNo(), dbInfo.getDbNo()));
                            return false;
                        }
                        oInfo.getBuffer().position(oInfo.getOffset() + 8);
                        contentLength = oInfo.getBuffer().getInt();
                    }

                    if (oInfo.getExpireTime() == -1 && contentLength == 0) {
                        //add new key & data to the key map
                        addNewData(dbInfo, oInfo, bucketNo, newKey, newExpireTime, newContentLength + 12, newByteBuffer, newVersion);
                    } else {
                        byte[] existedData = new byte[sizePer];
                        synchronized (oInfo.getBuffer()) {
                            if (dbInfo.getIsDispose()) {
                                LOG.error(String.format("syncDBIf | this bucket -> %d ,dbInfo-> %s has been disposed", dbInfo.getBucketNo(), dbInfo.getDbNo()));
                                return false;
                            }
                            oInfo.getBuffer().position(oInfo.getOffset());
                            oInfo.getBuffer().get(existedData, 0, sizePer);
                        }

                        //LOG.debug("replace happen, the old key[" + new String(existedKey) + "],the new key[" + new String(newKey) + "]");

                        replaceData(dbInfo, oInfo, bucketNo, newKey, newExpireTime, newContentLength + 12, newByteBuffer, existedData, newVersion);
                    }
                }
            }
        } catch (OutOfMaxCapacityException e) {
            LOG.error("OutOfMaxCapacityException in syncDBIf ", e);
            throw new HippoStoreException("OutOfMaxCapacityException in syncDBIf, bucket no -> " + bucketNo, HippoCodeDefine.HIPPO_BUCKET_OUT_MEMORY);
        } catch (Exception e2) {
            LOG.error("Unexpected exception in syncDBIf ", e2);
            throw new HippoStoreException("Unexpected exception in syncDBIf, bucket no -> " + bucketNo, HippoCodeDefine.HIPPO_SERVER_ERROR);
        } finally {
            if (dbInfo != null) {
                dbInfo.stopUsing();
            }
        }
        return true;
    }

    private void addNewData(DBInfo dbInfo, OffsetInfo oInfo, int bucketNo, byte[] newKey, long newExpireTime, int newContentLength, byte[] newByteBuffer, int version) throws HippoStoreException {
        if (newExpireTime != -1) {
            oInfo.setKLength(newKey.length);
            oInfo.setExpireTime(newExpireTime);
            dbInfo.decreaseFree();
            MdbPointer oldPoint = keyManager.getStoreInfo(newKey, bucketNo);
            if (oldPoint != null) {
                //change the old key's buffer time to -1, make sure they are the same key
                mdbManager.expireOffsetTimeWhenSyncData(oldPoint, newKey);

                oldPoint.setKLength(newKey.length);
                oldPoint.setLength(newContentLength);
                oldPoint.setDbNo(dbInfo.getDbNo());
                oldPoint.setBuckId(dbInfo.getBucketNo());
                oldPoint.setOffset(oInfo.getOffset());
                oldPoint.setVersion(version);

                //LOG.debug("new data has been replaced and need to remove the old data!! bucketNo -> " + bucketNo);
            } else {
                MdbPointer point = new MdbPointer(bucketNo, oInfo.getDbNo(), oInfo.getOffset(), newContentLength, version);
                point.setKLength(newKey.length);
                keyManager.setKeyStoreInfo(newKey, point, bucketNo);
            }
        } else {
            oInfo.setKLength(-1);
        }
        dbInfo.merge(oInfo.getOffset(), newByteBuffer);
    }

    private void replaceData(DBInfo dbInfo, OffsetInfo oInfo, int bucketNo, byte[] newKey, long newExpireTime, int newContentLength, byte[] newByteBuffer, byte[] oldByteBuffer, int newVersion) throws HippoStoreException {
        if (ByteUtil.isSame(newByteBuffer, oldByteBuffer)) {
            return;
        }

        //old data is not expire!!
        int existedKeyLength = Logarithm.bytesToInt(oldByteBuffer, 0);
        byte[] existedKey = Arrays.copyOfRange(oldByteBuffer, 12, 12 + existedKeyLength);
        byte[] existedContentLength = Arrays.copyOfRange(oldByteBuffer, 8, 12);
        int oldContentLength = Logarithm.bytesToInt(existedContentLength, 0);
        MdbPointer oldPoint = keyManager.getStoreInfo(existedKey, bucketNo);
        long existedExpireTime = Logarithm.getLong(oldByteBuffer, oldContentLength + 4);

        if (existedExpireTime != -1 && oldPoint != null) {
            if (dbInfo.getDbNo().equals(oldPoint.getDbNo()) && oldPoint.getOffset() == oInfo.getOffset()) {
                keyManager.removeStoreInfo(existedKey, bucketNo);
            }
        }

        MdbPointer oldNewKeyPoint = keyManager.getStoreInfo(newKey, bucketNo);
        if (newExpireTime == -1) {
            //the new data has been expired
            if (oldNewKeyPoint != null && dbInfo.getDbNo().equals(oldNewKeyPoint.getDbNo()) && oldNewKeyPoint.getOffset() == oInfo.getOffset()) {
                keyManager.removeStoreInfo(newKey, bucketNo);
            }

            if (existedExpireTime != -1) {
                dbInfo.getFreeCount().incrementAndGet();
            }
            oInfo.reset();
        } else {
            if (oldNewKeyPoint != null) {
                mdbManager.expireOffsetTimeWhenSyncData(oldNewKeyPoint, newKey);

                oldNewKeyPoint.setKLength(newKey.length);
                oldNewKeyPoint.setLength(newContentLength);
                oldNewKeyPoint.setDbNo(dbInfo.getDbNo());
                oldNewKeyPoint.setBuckId(dbInfo.getBucketNo());
                oldNewKeyPoint.setOffset(oInfo.getOffset());
                oldNewKeyPoint.setVersion(newVersion);
            } else {
                MdbPointer newPoint = new MdbPointer(bucketNo, oInfo.getDbNo(), oInfo.getOffset(), newContentLength, newVersion);
                newPoint.setKLength(newKey.length);
                keyManager.setKeyStoreInfo(newKey, newPoint, bucketNo);
            }
            oInfo.setExpireTime(newExpireTime);
            oInfo.setKLength(newKey.length);
        }
        oInfo.setRecycle(true, false);
        dbInfo.merge(oInfo.getOffset(), newByteBuffer);
    }

    @Override
    public void resetBuckets(List<BucketInfo> bucketIfs) {
        synchronized (bucketNos) {
            for (BucketInfo info : bucketNos) {
                Integer bucketNo = info.getBucketNo();
                boolean isContain = false;

                for (BucketInfo newInfo : bucketIfs) {
                    if (info.getBucketNo().intValue() == newInfo.getBucketNo().intValue()) {
                        isContain = true;
                        break;
                    }
                }

                if (!isContain) {
                    DBAssembleInfo dbAssembleInfo = assembleMap.get(bucketNo);
                    if (dbAssembleInfo != null) {
                        dbAssembleInfo.dispose();
                    }
                    assembleMap.remove(bucketNo);
                    LOG.info("DBAssembleManagerImpl | resetBuckets | DBAssembleInfo remove, sizeType -> " + sizeModel + ", bucketNo -> " + bucketNo);
                }
            }

            bucketNos.clear();
            bucketNos.addAll(bucketIfs);

            for (BucketInfo newInfo : bucketIfs) {
                Integer newBucketNo = newInfo.getBucketNo();
                DBAssembleInfo assembleInfo = assembleMap.putIfAbsent(newBucketNo, new DBAssembleInfo(sizeModel, mdbManager, newBucketNo, sizeMapping, expireLimit));
                if (assembleInfo == null) {
                    assembleMap.get(newBucketNo).initCreateDefaultDBInfo();
                    LOG.info("DBAssembleManagerImpl | resetBuckets | DBAssembleInfo added, the bucket -> " + newBucketNo + " , size -> " + sizeModel);
                }
            }
        }
    }

    @Override
    public void deleteDBIf(Integer bucketNo, String dbIfId) {
        DBAssembleInfo info = assembleMap.get(bucketNo);
        if (info != null) {
            info.deleteDBIf(dbIfId);
        }
    }

    @Override
    public Set<String> collectDBIfsBetweenGivenTime(Integer bucketNo, long beginTime, long endTime) {
        DBAssembleInfo dbAssembleInfo = assembleMap.get(bucketNo);
        return dbAssembleInfo.getKeysBetweenGivenTime(beginTime, endTime);
    }

    @Override
    public long getBucketLatestModifiedTime(Integer bucketNo) {
        long time = 0;
        DBAssembleInfo assembleInfo = assembleMap.get(bucketNo);
        if (assembleInfo != null) {
            time = assembleInfo.getLatestModifiedTime();
        }
        return time;
    }

    @Override
    public void setRecyclePoint(MdbPointer oldPoint) {
        DBAssembleInfo assembleInfo = assembleMap.get(oldPoint.getBuckId());
        if (assembleInfo != null) {
            DBInfo dbInfo = assembleInfo.findDBInfo(oldPoint.getDbNo());
            if (dbInfo != null) {
                OffsetInfo oInfo = dbInfo.findOffsetInfoByOffset(oldPoint.getOffset());
                if (oInfo != null) {
                    dbInfo.modifyExpireTimeInBuffer(oInfo, Logarithm.putLong(-1L));
                    oInfo.setRecycle(false, true);
                } else {
                    LOG.warn("cover the old point could not find the OffsetInfo, its offset is " + oldPoint.getOffset());
                }
            } else {
                LOG.warn("cover the old point could not find the dbInfo, its dbInfo is " + oldPoint.getDbNo());
            }
        } else {
            LOG.warn("cover the old point could not find the DBAssembleInfo, its bucketNo is " + oldPoint.getDbNo());
        }
    }

    @Override
    public List<SyncDataTask> getTasksNotInSyncList(Set<String> dbIfs, Integer bucketNo) throws HippoStoreException {
        List<SyncDataTask> result = new ArrayList<SyncDataTask>();
        DBAssembleInfo assembleInfo = assembleMap.get(bucketNo);
        if (assembleInfo != null) {
            for (String dbInfo : assembleInfo.getDbInfoMap().keySet()) {
                if (dbIfs != null) {
                    if (!dbIfs.contains(dbInfo)) {
                        result.add(new SyncDataTask(dbInfo, sizeModel));
                    }
                } else {
                    result.add(new SyncDataTask(dbInfo, sizeModel));
                }
            }
            return result;
        } else {
            throw new HippoStoreException("bucket -> " + bucketNo + " not existed!", HippoCodeDefine.HIPPO_BUCKET_NOT_EXISTED);
        }
    }

    @Override
    public boolean verifyExpiredDbIf(String dbInfoId, Integer bucketNo) {
        DBAssembleInfo assembleInfo = assembleMap.get(bucketNo);
        boolean isContain;
        if (assembleInfo != null) {
            isContain = assembleInfo.getDbInfoMap().containsKey(dbInfoId);
        } else {
            LOG.warn("bucket -> " + bucketNo + " not existed when getTasksNotInSyncList");
            isContain = false;
        }
        return isContain;
    }


    @Override
    public void expireOffset(DBInfo dbInfo, int offset, boolean modifyBuffer) {
        OffsetInfo oInfo = dbInfo.findOffsetInfoByOffset(offset);
        if (dbInfo.getIsDispose() || oInfo == null) {
            LOG.warn("the point refer to a dispose dbInfo! dbInfo id is " + dbInfo.getDbNo() + ", bucket no -> " + dbInfo.getBucketNo());
        } else {
            dbInfo.expireOffsetTime(oInfo, modifyBuffer);
            mdbManager.expireOffsetCallBack(dbInfo.getBucketNo(), dbInfo.getModifyTime());
        }
    }

    public DBAssembleInfo getDBAssembleInfo(int bucketNo) {
        return assembleMap.get(bucketNo);
    }

    public byte[] getKeyFromBuffer(MdbPointer pointer) {
        DBAssembleInfo info = assembleMap.get(pointer.getBuckId());
        DBInfo dbInfo = info.getDbInfoMap().get(pointer.getDbNo());
        if (dbInfo != null) {
            FetchBean fetchBean = dbInfo.fetchMemory(pointer.getOffset(), pointer.getLength());
            return BufferUtil.separateKey(fetchBean.getOriginData(), MdbConstants.HEADER_LENGTH_FOR_INT * 3, fetchBean.getKLength());
        } else {
            return null;
        }
    }
}
