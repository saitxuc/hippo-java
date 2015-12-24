package com.hippo.mdb.obj;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.util.Logarithm;
import com.hippo.mdb.BlockSizeMapping;
import com.hippo.mdb.Lru;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbManager;
import com.hippo.mdb.exception.OutOfMaxCapacityException;
import com.hippo.mdb.impl.FastPeriodsLRU;
import com.hippo.store.exception.HippoStoreException;

/**
 * @author saitxuc
 *         write 2014-7-28
 */
public class DBAssembleInfo {

    /**
     *
     */
    protected static final Logger LOG = LoggerFactory.getLogger(DBAssembleInfo.class);

    private volatile DBInfo currentDB;

    /**
     * exclude currentDB
     */
    private final ConcurrentHashMap<String, DBInfo> dbInfoMap = new ConcurrentHashMap<String, DBInfo>();

    private MdbManager mdbManager;

    private String sizeModel;

    private Lru lru = null;

    private AtomicBoolean doing = new AtomicBoolean(false);

    private AtomicBoolean stopFlag = new AtomicBoolean(false);

    private final Object mutex = new Object();

    private final LinkedList<OffsetInfo> sortedLRUList = new LinkedList<OffsetInfo>();

    private final LinkedList<String> freeDBInfo = new LinkedList<String>();

    private AtomicBoolean useLRUData = new AtomicBoolean(false);

    private Integer bucketNo;

    private final LinkedList<String> dbIfsSnapShot = new LinkedList<String>();

    private Set<String> expiredTasks = new HashSet<String>();

    private final Set<String> lruTasks = new HashSet<String>();

    private BlockSizeMapping sizeMapping;

    private int expireLimit = 10;

    public DBAssembleInfo(String sizeModel, MdbManager mdbManager, Integer bucketNo, BlockSizeMapping sizeMapping, int expireLimit) {
        this.sizeModel = sizeModel;
        this.mdbManager = mdbManager;
        this.lru = new FastPeriodsLRU(this);
        this.bucketNo = bucketNo;
        this.sizeMapping = sizeMapping;
        this.expireLimit = expireLimit;
    }

    public LinkedList<OffsetInfo> getSortedLRUList() {
        return sortedLRUList;
    }

    /**
     * used to init the dbinfo,  the infos created in this method will never be dispose
     */
    public void initCreateDefaultDBInfo() {
        try {
            for (int i = 1; i <= 2; i++) {
                DBInfo newDb = findOrCreateDBInfo(bucketNo + "_" + sizeModel + "_inited_" + i, bucketNo);
                newDb.setModifiedTime(0L);
                newDb.setKeepFlag(true);
                //currentDB = newDb;
                LOG.info("DBAssembleInfo init to create an new DBinfo , the id is " + newDb.getDbNo() + " , size is " + sizeModel + " k");
            }
        } catch (OutOfMaxCapacityException e) {
            LOG.error("error when init the DBAssembleInfo", e);
        }
    }

    public OffsetInfo getCurrentDBOffset() throws HippoStoreException {
        OffsetInfo offsetInfo = null;
        for (; ; ) {
            if (doing.get()) {
                synchronized (mutex) {
                    try {
                        mutex.wait(500);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            if (stopFlag.get()) {
                return null;
            }

            if (useLRUData.get()) {
                synchronized (sortedLRUList) {
                    if (sortedLRUList.size() > 0) {
                        offsetInfo = sortedLRUList.removeFirst();
                        offsetInfo.setLru(true);
                        return offsetInfo;
                    }
                }
            }

            if ((currentDB == null || currentDB.isFull())) {
                if (doing.compareAndSet(false, true)) {
                    DBInfo newDb = null;
                    try {
                        synchronized (freeDBInfo) {
                            Iterator<String> iter = freeDBInfo.iterator();
                            while (iter.hasNext()) {
                                String dbNo = iter.next();
                                iter.remove();
                                newDb = dbInfoMap.get(dbNo);
                                if (newDb != null) {
                                    //newDb.waitingExpiredEnd();
                                    if (newDb.startUsing()) {
                                        if (newDb.isExpiring()) {
                                            newDb.stopUsing();
                                            continue;
                                        }
                                    }

                                    if (newDb.isFree()) {
                                        offsetInfo = newDb.getAvailableOffset();
                                        return offsetInfo;
                                    }
                                }
                            }
                        }

                        newDb = createCurrentDB(null, bucketNo);

                        if (newDb.isFree()) {
                            newDb.startUsing();
                            offsetInfo = newDb.getAvailableOffset();
                            synchronized (dbInfoMap) {
                                if (stopFlag.get()) {
                                    return null;
                                } else {
                                    dbInfoMap.put(newDb.getDbNo(), newDb);
                                }
                            }
                            return offsetInfo;
                        }
                    } catch (OutOfMaxCapacityException e) {
                        boolean isNeedLRU = true;
                        synchronized (sortedLRUList) {
                            if (sortedLRUList.size() > 0) {
                                isNeedLRU = false;
                                offsetInfo = sortedLRUList.pollFirst();
                                offsetInfo.setLru(true);
                            }
                        }

                        if (isNeedLRU) {
                            offsetInfo = lru.lru();
                            offsetInfo.setLru(true);
                        }
                        useLRUData.compareAndSet(false, true);

                        return offsetInfo;
                    } finally {
                        if (newDb != null) {
                            DBInfo oldDB = currentDB;
                            if (oldDB != null && !oldDB.getDbNo().equals(newDb.getDbNo())) {
                                oldDB.stopUsing();
                            }
                            currentDB = newDb;
                        }
                        doing.set(false);
                    }
                }
            } else {
                try {
                    if (currentDB.isFree()) {
                        offsetInfo = currentDB.getAvailableOffset();
                        return offsetInfo;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public DBInfo getCurrentDB() {
        return this.currentDB;
    }

    /**
     * @param createDbNo
     * @param bucketNo
     * @return
     * @throws OutOfMaxCapacityException
     */
    private synchronized DBInfo createCurrentDB(String createDbNo, Integer bucketNo) throws OutOfMaxCapacityException {
        DBInfo info = mdbManager.fetchNewDBInfo(MdbConstants.CAPACITY_SIZE, sizeModel, createDbNo, bucketNo);
        info.init();
        return info;
    }

    /**
     * for slave to sync data and fetch the info
     *
     * @param createDbNo
     * @param bucketNo
     * @return
     * @throws OutOfMaxCapacityException
     */
    public DBInfo findOrCreateDBInfo(String createDbNo, Integer bucketNo) throws OutOfMaxCapacityException {
        synchronized (dbInfoMap) {
            DBInfo info = dbInfoMap.get(createDbNo);
            if (info == null) {
                info = createCurrentDB(createDbNo, bucketNo);
                dbInfoMap.put(createDbNo, info);
            }
            return info;
        }
    }

    public DBInfo findDBInfo(String createDbNo) {
        return dbInfoMap.get(createDbNo);
    }

    public Map<String, DBInfo> getDbInfoMap() {
        return dbInfoMap;
    }

    public String getSizeModel() {
        return sizeModel;
    }

    public void dispose() {
        try {
            stopFlag.compareAndSet(false, true);

            sortedLRUList.clear();
            freeDBInfo.clear();
            expiredTasks.clear();

            synchronized (mutex) {
                mutex.notifyAll();
            }

            synchronized (dbInfoMap) {
                for (DBInfo dbInfo : dbInfoMap.values()) {
                    dbInfo.stop();
                }
                dbInfoMap.clear();
            }
        } catch (Exception e) {
            LOG.error("dispose error", e);
        }

    }

    private void clearLRUSet() {
        synchronized (lruTasks) {
            lruTasks.clear();
        }

        synchronized (sortedLRUList) {
            sortedLRUList.clear();
        }
    }

    private void prepareForLRU(boolean removeOld) {
        synchronized (dbIfsSnapShot) {
            if (removeOld) {
                lruTasks.clear();
            }

            int allCount = dbInfoMap.size();

            if (dbIfsSnapShot.size() == 0) {
                dbIfsSnapShot.addAll(dbInfoMap.keySet());
                dbIfsSnapShot.removeAll(expiredTasks);
            }

            int count = 0;

            for (; ; ) {
                String dbInfo = dbIfsSnapShot.pollFirst();

                if (dbInfo == null) {
                    break;
                }

                if (!expiredTasks.contains(dbInfo)) {
                    lruTasks.add(dbInfo);
                    count++;
                } else {
                    continue;
                }

                if (allCount < expireLimit) {
                    if (count >= allCount / 2) {
                        break;
                    }
                } else {
                    if (count >= expireLimit) {
                        break;
                    }
                }
            }
        }
    }

    public void callLRU() throws HippoStoreException {
        int currentUseCount = 0;

        //when the sort List has been full used will change
        if (sortedLRUList.size() == 0) {

            prepareForLRU(true);

            synchronized (lruTasks) {
                Iterator<String> lruIter = lruTasks.iterator();

                DBInfo dbInfo = null;

                while (lruIter.hasNext()) {
                    String dbInfoId = lruIter.next();

                    if (dbInfoId != null) {
                        dbInfo = dbInfoMap.get(dbInfoId);
                    }

                    try {
                        if (dbInfo != null && !dbInfo.isUsing()) {
                            List<OffsetInfo> result = dbInfo.getLruBlocksAfterLru();

                            if (result == null) {
                                continue;
                            }

                            synchronized (sortedLRUList) {
                                sortedLRUList.addAll(result);
                            }

                            result.clear();
                        }
                    } catch (Exception e) {
                        LOG.error("error when do expire for lru , id -> " + dbInfoId, e);
                    }
                }
            }
        }

        //go through the lru but could not get the offset
        if (sortedLRUList.size() == 0) {
            if (currentDB != null) {
                LOG.warn(currentDB.getDbNo() + " , LRU could not fetch offset, current dbinfo map size -> " + dbInfoMap.size() + ", expireTasks size -> " + expiredTasks
                        .size() + ", lruTasks size -> " + lruTasks.size() + ", current size model -> " + sizeModel + ", current use count -> " + currentUseCount);
            }

            //fetch current DFInfo
            if (currentDB != null) {
                DBInfo info = currentDB;
                lruTasks.add(info.getDbNo());
                synchronized (sortedLRUList) {
                    sortedLRUList.addAll(info.getLruBlocksAfterLru());
                }
                info.stopUsing();
                /*System.out.println(info.getDbNo() + " was called stop using!");*/
                currentDB = null;
            } else {
                LOG.warn("could not get the currentDB");
            }
        }
    }

    private void prepareForExpire(boolean removeOld) {
        synchronized (dbIfsSnapShot) {
            if (removeOld) {
                expiredTasks.clear();
            }

            int allCount = dbInfoMap.size();

            if (dbIfsSnapShot.size() == 0) {
                dbIfsSnapShot.addAll(dbInfoMap.keySet());
                dbIfsSnapShot.removeAll(lruTasks);
                DBInfo currentDb = currentDB;
                if (currentDb != null) {
                    dbIfsSnapShot.remove(currentDb.getDbNo());
                }
            }

            int count = 0;
            for (; ; ) {
                String dbInfo = dbIfsSnapShot.pollFirst();

                if (dbInfo == null) {
                    break;
                }

                if (!lruTasks.contains(dbInfo)) {
                    expiredTasks.add(dbInfo);
                    count++;
                } else {
                    continue;
                }

                if (allCount < expireLimit) {
                    if (count >= allCount / 2) {
                        break;
                    }
                } else {
                    if (count >= expireLimit) {
                        break;
                    }
                }
            }
        }
    }

    public List<String> expire() {
        if (stopFlag.get()) {
            LOG.info("DBAssembleInfo detect stop flag");
            return null;
        }

        synchronized (freeDBInfo) {
            freeDBInfo.clear();
        }

        List<String> disposeDBInfo = new ArrayList<String>();
        List<String> freeDBs = new ArrayList<String>();

        prepareForExpire(true);

        for (String expiredTask : expiredTasks) {

            if (stopFlag.get()) {
                LOG.info("DBAssembleInfo detect stop flag");
                return null;
            }

            DBInfo dbInfo = dbInfoMap.get(expiredTask);
            if (dbInfo != null && !dbInfo.isUsing()) {
                dbInfo.sortOffsetsAndExpired();
                if (dbInfo.getIsDispose()) {
                    synchronized (dbInfoMap) {
                        dbInfoMap.remove(dbInfo.getDbNo());
                        disposeDBInfo.add(dbInfo.getDbNo());
                    }
                } else {
                    if (!dbInfo.isFull()) {
                        freeDBs.add(dbInfo.getDbNo());
                    }
                }
            }
        }

        if ((!mdbManager.memoryCheckFull(bucketNo) || freeDBs.size() > 0)) {
            clearLRUSet();
            synchronized (freeDBInfo) {
                freeDBInfo.addAll(freeDBs);
            }
            freeDBs.clear();
            if (useLRUData.compareAndSet(true, false)) {
                LOG.info("detect the free offset , LRU model end!! bucket ->" + bucketNo + ", has clear the lru set");
            }
        }
        //System.out.println("expire end!! done -> " + expiredTasks);
        return disposeDBInfo;
    }

    /**
     * copy the data
     *
     * @param dbIfId
     * @param offset
     * @param size
     * @return
     */
    public byte[] duplicateDirectBuffer(String dbIfId, int offset, int size) {
        DBInfo dbInfo = dbInfoMap.get(dbIfId);
        if (dbInfo != null) {
            if (dbInfo.getIsDispose()) {
                return null;
            } else {
                return dbInfo.duplicate(offset, size);
            }
        } else {
            return null;
        }
    }

    /**
     * delete the dbinfo content and remove the key in the key manager
     * @param dbIfId
     */
    public void deleteDBIf(String dbIfId) {
        DBInfo del = dbInfoMap.get(dbIfId);

        if (del != null) {
            del.setSyncDispose();
            ByteBuffer buffer = del.getByteBuffer();
            int sizePer = sizeMapping.getSIZE_PER(sizeModel);
            int count = sizeMapping.getSIZE_COUNT(sizeModel);
            for (int index = 0; index < count; index++) {
                int offsetInByte = index * sizePer;
                byte[] data = new byte[sizePer];

                synchronized (buffer) {
                    if (del.getIsDispose()) {
                        LOG.error(String.format("deleteDBIf | this bucket -> %d, dbinfo -> %s has been disposed", del.getBucketNo(), del.getDbNo()));
                        return;
                    }
                    buffer.position(offsetInByte);
                    buffer.get(data, 0, sizePer);
                }

                int keyLength = Logarithm.bytesToInt(data, 0);
                int contentLength = Logarithm.bytesToInt(data, 8);
                byte key[] = Arrays.copyOfRange(data, 12, 12 + keyLength);
                long expireTime = Logarithm.getLong(data, contentLength + 4);
                if (expireTime != -1) {
                    if (key.length > 0) {
                        mdbManager.removeKeyFromMap(key, del, offsetInByte);
                    }
                }

            }
            del.dispose();
        }

        synchronized (dbInfoMap) {
            dbInfoMap.remove(dbIfId);
        }
    }

    public Set<String> getKeysBetweenGivenTime(long beginTime, long endTime) {
        Set<String> sets = new HashSet<String>();
        synchronized (dbInfoMap) {
            for (DBInfo info : dbInfoMap.values()) {
                long currentTime = info.getModifyTime();
                if (currentTime > beginTime && currentTime <= endTime) {
                    sets.add(info.getDbNo());
                }
            }
            return sets;
        }
    }

    public long getLatestModifiedTime() {
        long time = 0;
        synchronized (dbInfoMap) {
            for (DBInfo info : dbInfoMap.values()) {
                long modifyTime = info.getModifyTime();
                time = Math.max(time, modifyTime);
            }
        }
        return time;
    }

}
