package com.pinganfu.hippo.mdb.obj;

import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.util.Logarithm;
import com.pinganfu.hippo.mdb.BlockSizeMapping;
import com.pinganfu.hippo.mdb.MdbConstants;
import com.pinganfu.hippo.mdb.MdbManager;
import com.pinganfu.hippo.mdb.utils.DBInfoUtil;

/**
 * @author saitxuc
 *         write 2014-7-28
 */
public class DBInfo {

    protected static final Logger LOG = LoggerFactory.getLogger(DBInfo.class);

    private final LinkedList<OffsetInfo> offsets = new LinkedList<OffsetInfo>();

    private String dbNo;

    private ByteBuffer byteBuffer;

    private AtomicInteger freeCount = new AtomicInteger(0);

    private int capacity;

    private String sizeModel;

    private volatile long modifyTime;

    private AtomicBoolean expiring = new AtomicBoolean(false);

    private int LRU_COUNT = 0;

    private MdbManager mdbManager;

    private Integer bucketNo;

    private float lruFate;

    private AtomicBoolean isDispose = new AtomicBoolean(false);

    private AtomicBoolean syncDispose = new AtomicBoolean(false);

    private AtomicBoolean using = new AtomicBoolean(false);

    private final Object waitingForExpiring = new Object();

    private BlockSizeMapping sizeMapping;

    private boolean keepFlag = false;

    public DBInfo(int capacity, String sizeModel, ByteBuffer buffer, String dbNo, MdbManager mdbManager, Integer bucketNo, float lruFate,
                  BlockSizeMapping sizeMapping) {
        this.sizeModel = sizeModel;
        this.capacity = capacity;
        this.byteBuffer = buffer;
        this.dbNo = dbNo;
        this.mdbManager = mdbManager;
        this.bucketNo = bucketNo;
        this.lruFate = lruFate;
        this.sizeMapping = sizeMapping;
    }

    public void init() {
        Integer count = sizeMapping.getSIZE_COUNT(sizeModel);
        Integer perSize = sizeMapping.getSIZE_PER(sizeModel);

        if (count == null) {
            throw new RuntimeException("no find size model in size referred , the error size model is " + sizeModel);
        }

        LRU_COUNT = (int) Math.ceil((double) count * (double) lruFate);

        freeCount = new AtomicInteger(count);

        for (int i = 0; i < count; i++) {
            int offset = i * perSize;
            OffsetInfo OffsetInfo = new OffsetInfo(this.dbNo, offset, -1, this.byteBuffer);
            offsets.add(OffsetInfo);
        }
    }

    public FetchBean fetchMemory(int position, int length) {
        if (position < 0) {
            throw new IllegalArgumentException("Position not less than zero!");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be greater than 0!");
        }
        synchronized (byteBuffer) {
            if (getIsDispose()) {
                throw new SecurityException(String.format("DBInfo | fetchMemory | this bucket -> %d ,dbinfo ->  %s has been disposed, poistion -> %d , should not be used again!", this.bucketNo, this.dbNo, position));
            }
            final byte[] dest = new byte[length];
            byteBuffer.position(position);
            int kLength = this.byteBuffer.getInt();
            byteBuffer.position(position);
            byteBuffer.get(dest, 0, length);
            return new FetchBean(dest, kLength);
        }
    }
    
    public FetchBitBean fetchBitMemory(int position, int offset, int separatorLength) {
        if (position < 0) {
            throw new IllegalArgumentException("Position not less than zero!");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Length must be greater than 0!");
        }
        if (separatorLength <= 0) {
            throw new IllegalArgumentException("separatorLength must be greater than 0!");
        }

        synchronized (this.byteBuffer) {
            if (getIsDispose()) {
                throw new SecurityException(String.format("DBInfo | fetchMemory | this bucket -> %d ,dbinfo ->  %s has been disposed, poistion -> %d , should not be used again!", this.bucketNo, this.dbNo, position));
            }
            this.byteBuffer.position(position);
            int kLength = this.byteBuffer.getInt();
            int version = this.byteBuffer.getInt();
            int contentLength = this.byteBuffer.getInt();
            final byte[] key = new byte[kLength];
            final byte[] expireTime = new byte[8];
            this.byteBuffer.position(position + 12);
            this.byteBuffer.get(key, 0, kLength);
            this.byteBuffer.position(position + 12 + kLength + separatorLength + offset);
            byte data = this.byteBuffer.get();
            this.byteBuffer.position(position + 4 + contentLength);
            this.byteBuffer.get(expireTime, 0, 8);
            return new FetchBitBean(data, expireTime, version , key);
        }
    }

    public boolean isFree() {
        try {
            return (freeCount.decrementAndGet() >= 0);
        } finally {
            if (freeCount.get() < 0) {
                freeCount.set(0);
            }
        }
    }

    public boolean isFull() {
        return (freeCount.get() <= 0);
    }

    public long getModifyTime() {
        return modifyTime;
    }

    public void decreaseFree() {
        freeCount.decrementAndGet();
    }

    public OffsetInfo getAvailableOffset() {
        synchronized (offsets) {
            OffsetInfo offsetInfo = offsets.removeFirst();
            offsetInfo.setLru(false);
            offsets.addLast(offsetInfo);
            return offsetInfo;
        }
    }

    public void stop() {
        waitingExpiredEnd();
        if (isDispose.compareAndSet(false, true)) {
            synchronized (offsets) {
                offsets.clear();
            }
            synchronized (byteBuffer) {
                if (byteBuffer.isDirect()) {
                    PlatformDependent.freeDirectBuffer(byteBuffer);
                }
            }
            mdbManager.reduceCurrentCapacity(capacity, bucketNo);
        }
    }

    public void dispose() {
        if (isDispose.compareAndSet(false, true)) {
            synchronized (offsets) {
                offsets.clear();
            }
            synchronized (byteBuffer) {
                if (byteBuffer.isDirect()) {
                    PlatformDependent.freeDirectBuffer(byteBuffer);
                }
            }
            mdbManager.reduceCurrentCapacity(capacity, bucketNo);
        }
    }

    public void expireOffsetTime(OffsetInfo oInfo, boolean modifyBuffer) {
        oInfo.reset();
        if (modifyBuffer) {
            modifyExpireTimeInBuffer(oInfo, Logarithm.putLong(-1));
        }
        updateModifyTime();
    }

    public void modifyExpireTimeInBuffer(OffsetInfo oInfo, byte[] expireTime) {
        ByteBuffer buffer = oInfo.getBuffer();
        synchronized (buffer) {
            if (getIsDispose()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("modifyExpireTimeInBuffer | detect the dbInfo -> " + dbNo + " has been disposed!");
                }
                return;
            }

            buffer.position(oInfo.getOffset() + 8);

            int contentLength = buffer.getInt();
            if (contentLength != 0) {
                buffer.position(oInfo.getOffset() + contentLength + 4);
                buffer.put(expireTime);
            }
        }
    }

    public synchronized void updateModifyTime() {
        Date now = new Date();
        this.modifyTime = now.getTime();
    }

    public byte[] duplicate(int offset, int size) {
        byte[] data;

        if (offset + size > MdbConstants.CAPACITY_SIZE) {
            size = MdbConstants.CAPACITY_SIZE - offset;
        }

        data = new byte[size + 8];

        synchronized (byteBuffer) {
            if (getIsDispose()) {
                LOG.warn("modifyExpireTimeInBuffer | detect the dbInfo -> " + dbNo + " has been disposed!!");
                return null;
            }

            byteBuffer.position(offset);
            byteBuffer.get(data, 0, size);
        }

        byte[] modifyTime = Logarithm.putLong(this.modifyTime);

        System.arraycopy(modifyTime, 0, data, size, 8);

        return data;
    }

    public void merge(int offset, byte[] data) {
        synchronized (byteBuffer) {
            if (getIsDispose()) {
                throw new IllegalArgumentException(String.format("merge | detect the bucket no -> %d, dbinfo -> %s has been disposed", bucketNo, dbNo));
            }

            byteBuffer.position(offset);
            byteBuffer.put(data);
        }
    }

    public boolean isExpiring() {
        return expiring.get();
    }

    public OffsetInfo findOffsetInfoByOffset(int offset) {
        synchronized (offsets) {
            for (OffsetInfo oInfo : offsets) {
                if (oInfo.getOffset() == offset) {
                    return oInfo;
                }
            }
        }
        return null;
    }

    /**
     * 处理过期并且排序并且返回队列的固定比例数量的块
     */
    public void sortOffsetsAndExpired() {
        if (expiring.compareAndSet(false, true)) {
            try {
                if (!using.get()) {
                    long currentTime = new Date().getTime();
                    int count = offsets.size();
                    List<DeleteItem> deleteItems = new ArrayList<DeleteItem>();
                    synchronized (offsets) {
                        for (OffsetInfo oInfo : offsets) {
                            if (oInfo.getExpireTime() == -1) {
                                count--;
                                continue;
                            }

                            //this older block data could be recycle
                            if (oInfo.setRecycle(true, false)) {
                                mdbManager.expireOffsetTime(this, oInfo.getOffset());
                                this.freeCount.incrementAndGet();
                                count--;
                                continue;
                            }

                            if (oInfo.getExpireTime() <= currentTime && oInfo.getExpireTime() != -1) {
                                int beginIndex = oInfo.getOffset();
                                byte[] key;
                                count--;
                                synchronized (byteBuffer) {
                                    if (getIsDispose()) {
                                        LOG.warn("sortOffsetsAndExpired | find the dispose flag when do the dbinfo -> " + dbNo);
                                        return;
                                    }
                                    byteBuffer.position(beginIndex);
                                    int keyLength = byteBuffer.getInt();
                                    key = new byte[keyLength];
                                    byteBuffer.position(beginIndex + 12);
                                    byteBuffer.get(key, 0, keyLength);
                                }

                                if (key.length > 0) {
                                    deleteItems.add(new DeleteItem(key, oInfo.getOffset()));
                                    this.freeCount.incrementAndGet();
                                }
                            }
                        }
                    }

                    for (DeleteItem del : deleteItems) {
                        if (getIsDispose()) {
                            LOG.warn("dbInfo -> " + dbNo + " has been disposed, the expire will be ignore!");
                            return;
                        }
                        mdbManager.deleteByKey(del.getKey(), this, del.getOffset());
                    }

                    deleteItems.clear();

                    if (count == 0 && !keepFlag) {
                        dispose();
                        LOG.info(dbNo + " from bucket No -> " + bucketNo + " has been released!!");
                        return;
                    } else {
                        //fix the count if the number not right
                        if (offsets.size() - count != freeCount.get()) {
                            this.freeCount.set(offsets.size() - count);
                        }
                    }

                    synchronized (offsets) {
                        DBInfoUtil.sortOffsets(offsets);
                    }
                }
            } finally {
                expiring.compareAndSet(true, false);
                synchronized (waitingForExpiring) {
                    waitingForExpiring.notifyAll();
                }
            }
        }
    }

    public boolean getIsDispose() {
        return isDispose.get();
    }

    public boolean setSyncDispose() {
        return syncDispose.compareAndSet(false, true);
    }

    public boolean getSyncDispose() {
        return syncDispose.get();
    }

    public boolean isUsing() {
        return using.get();
    }

    /**
     * sort and return lru list
     * used by FastPeriodsLRU
     */
    public List<OffsetInfo> getLruBlocksAfterLru() {
        if (expiring.compareAndSet(false, true)) {
            try {
                List<OffsetInfo> result = new ArrayList<OffsetInfo>();
                int count = offsets.size();
                synchronized (offsets) {
                    for (OffsetInfo oInfo : offsets) {
                        if (oInfo.getExpireTime() == -1) {
                            count--;
                            continue;
                        }

                        //this older block data could be recycle
                        if (oInfo.setRecycle(true, false)) {
                            mdbManager.expireOffsetTime(this, oInfo.getOffset());
                            count--;
                            this.freeCount.incrementAndGet();
                        }
                    }

                    if (offsets.size() - count != freeCount.get()) {
                        this.freeCount.set(offsets.size() - count);
                    }

                    DBInfoUtil.sortOffsets(offsets);

                    result.addAll(offsets.subList(0, LRU_COUNT));
                }
                return result;
            } finally {
                expiring.compareAndSet(true, false);
            }
        } else {
            return null;
        }
    }

    /**
     * used by FastExpireLru begin
     */
    public OffsetInfo getBlockAfterLru() {
        OffsetInfo offsetInfo;
        synchronized (offsets) {
            DBInfoUtil.sortOffsets(offsets);
            offsetInfo = offsets.removeFirst();
        }
        return offsetInfo;
    }

    public OffsetInfo getMinExpire() {
        OffsetInfo offsetInfo;
        synchronized (offsets) {
            //offsets is over
            if (offsets.size() != 0) {
                offsetInfo = offsets.removeFirst();
                offsets.add(offsetInfo);
            } else {
                offsetInfo = getBlockAfterLru();
                if (offsets != null) {
                    offsets.add(offsetInfo);
                    freeCount.incrementAndGet();
                }
            }
        }
        return offsetInfo;
    }

    /**
     * used by FastExpireLru end
     */

    public synchronized void setModifiedTime(long newTime) {
        if (modifyTime < newTime) {
            modifyTime = newTime;
        }
    }

    public boolean startUsing() {
        return using.compareAndSet(false, true);
    }

    public boolean stopUsing() {
        return using.compareAndSet(true, false);
    }

    private void waitingExpiredEnd() {
        for (; ; ) {
            if (expiring.compareAndSet(false, true)) {
                return;
            } else {
                synchronized (waitingForExpiring) {
                    try {
                        waitingForExpiring.wait(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    public void setKeepFlag(boolean keepFlag) {
        this.keepFlag = keepFlag;
    }

    public String getDbNo() {
        return dbNo;
    }

    public void setDbNo(String dbNo) {
        this.dbNo = dbNo;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public AtomicInteger getFreeCount() {
        return freeCount;
    }

    public int getCapacity() {
        return capacity;
    }

    public String getSizeModel() {
        return sizeModel;
    }

    public void setSizeModel(String sizeModel) {
        this.sizeModel = sizeModel;
    }

    public Integer getBucketNo() {
        return bucketNo;
    }

    public void setBucketNo(Integer bucketNo) {
        this.bucketNo = bucketNo;
    }
}
