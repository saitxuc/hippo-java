package com.test.mdb;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.hippo.common.domain.BucketInfo;
import com.hippo.common.util.Logarithm;
import com.hippo.mdb.BlockSizeMapping;
import com.hippo.mdb.DBAssembleManager;
import com.hippo.mdb.KeyManager;
import com.hippo.mdb.MdbConstants;
import com.hippo.mdb.MdbStoreEngine;
import com.hippo.mdb.impl.DBAssembleManagerImpl;
import com.hippo.mdb.impl.MdbManagerImpl;
import com.hippo.mdb.obj.DBAssembleInfo;
import com.hippo.mdb.obj.DBInfo;
import com.hippo.mdb.obj.MdbPointer;
import com.hippo.mdb.utils.BufferUtil;
import com.test.mdb.impl.OffHeapKeyManagerTest;


public class MdbStoreEnginePerfTest {
    final static byte[] msgContent = ("content for test").getBytes();
    final static AtomicLong successCount = new AtomicLong(0);
    final static AtomicLong exceptionCount = new AtomicLong(0);
    final static AtomicLong allCount = new AtomicLong(0);
    final static int threadCount = 10;
    final static ExecutorService service = Executors.newFixedThreadPool(threadCount);
    final static int maxCount = 1024 * 1024 * 10;//10000000
    static BlockSizeMapping mapping = null;

    static {
        List<Double> sizeTypes = new ArrayList<Double>();
        for (double index = 0; ; index++) {
            Double size = Math.pow(MdbConstants.SIZE_FACTOR, index);
            if (size > MdbConstants.SIZE_LIMIT) {
                break;
            } else {
                sizeTypes.add(size);
            }
        }
        mapping = new BlockSizeMapping(sizeTypes);
    }

    public static void main(final String[] args) throws Exception {

        final Random ran = new Random();

        final MdbStoreEngine engine = new MdbStoreEngine();
        engine.setLimit(1024L * 1024L * 1024L);
        engine.init();

        MdbManagerImpl manager = (MdbManagerImpl) engine.getMdbManager();

        KeyManager keyManager = null;
        Class self = (Class) manager.getClass();
        Field[] fs = self.getDeclaredFields();
        for (int i = 0; i < fs.length; i++) {
            Field f = fs[i];
            f.setAccessible(true);
            Object val = f.get(manager);
            if (f.getName().equals("keyManager")) {
                keyManager = new OffHeapKeyManagerTest(manager,engine.getBuckets());
                f.set(manager,keyManager);
                System.out.println("find the keymanager!!");
            }
        }
        engine.start();
        engine.startPeriodsExpire();

        final CountDownLatch cd = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        cd.await();
                        while (allCount.incrementAndGet() <= maxCount) {
                            String key = "k" + ran.nextInt(2147483647);
                            //String key = UUID.randomUUID().toString();
                            boolean status = engine.addData(key.getBytes(), msgContent, 1000, 0);
                            if (status) {
                                successCount.incrementAndGet();
                            } else {
                                exceptionCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptionCount.incrementAndGet();
                    }
                }
            });
        }
        cd.countDown();

        long start = System.currentTimeMillis();
        while (allCount.get() <= (maxCount + 1000000000000L)) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            String str = "use the default param: ojbect--8111--set--10--1--1000000";
            if (allCount.get() > maxCount) {
                verifyKeyForTest(engine, keyManager);
                verifyBufferForTest(engine, keyManager);
                break;
            } else {
                System.out
                        .println("param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                                .currentTimeMillis() - start)) + "],size->" + engine.size() + ",capacity->" + engine.getCurrentUsedCapacity() + ",bucketCapacity->" + engine
                                .getBucketUsedCapacity(0));
            }
        }

        Thread.sleep(10000L);
        service.shutdown();
        System.out.println("----DB--count------->>>>>>>" + engine.getDataCount());
        engine.stop();
    }

    private static void verifyKeyForTest(MdbStoreEngine engine, KeyManager keyManager) {
        Set<Integer> buckets = new HashSet<Integer>();

        MdbManagerImpl manager = (MdbManagerImpl) engine.getMdbManager();

        for (BucketInfo bucketInfo : engine.getBuckets()) {
            buckets.add(bucketInfo.getBucketNo());
        }

        System.out.println("verifyKeyForTest begin.....................................");

        int error = 0;

        for (Integer bucketNo : buckets) {
            Set<Map.Entry<byte[], MdbPointer>> set = keyManager.getEntrySet(bucketNo);
            for (Map.Entry<byte[], MdbPointer> entry : set) {
                MdbPointer point = entry.getValue();
                if (point.getBuckId() != bucketNo) {
                    error++;
                    continue;
                }

                final String sizeFlag = BufferUtil.getSizePeriod(point.getLength(), mapping);

                DBAssembleManager dbManager = manager.getDbAssembleMap().get(sizeFlag);

                boolean right = ((DBAssembleManagerImpl) dbManager).verifyKey(entry.getKey(), point);

                if (!right) {
                    byte[] key = ((DBAssembleManagerImpl) dbManager).getKeyFromBuffer(point);
                    if (key != null) {
                        MdbPointer o_pointer = keyManager.getStoreInfo(key, 0);
                        if (o_pointer != null) {
                            boolean aa = ((DBAssembleManagerImpl) dbManager).verifyKey(key, o_pointer);
                            System.out
                                    .println(new String(key) + "--" + aa + " -- " + o_pointer.getDbNo() + " -- " + o_pointer.getOffset() + ", old point -> " + point
                                            .getDbNo() + " -- " + point.getOffset());
                        }
                        System.out
                                .println("------------------------detect error!! key -> " + new String(entry.getKey()) + " key in buffer is -> " + new String(key) + ", sizeFlag -> " + sizeFlag + ", bucketNo no -> " + bucketNo);
                        error++;
                    } else {
                        System.out.println("could not get the key , the dbInfoId may be deleted!!");
                    }
                }
            }
        }
        System.out.println("verifyKeyForTest end..................................... error ---------------> " + error);
    }

    private static void verifyBufferForTest(MdbStoreEngine engine, KeyManager keyManager) {
        Set<Integer> buckets = new HashSet<Integer>();
        MdbManagerImpl manager = (MdbManagerImpl) engine.getMdbManager();

        for (BucketInfo bucketInfo : engine.getBuckets()) {
            buckets.add(bucketInfo.getBucketNo());
        }

        System.out.println("verifyBufferForTest begin.....................................");

        int error = 0;

        for (Map.Entry<String, DBAssembleManager> entry : manager.getDbAssembleMap().entrySet()) {
            String size = entry.getKey();
            DBAssembleManager dbManager = entry.getValue();
            for (Integer bucketNo : buckets) {
                DBAssembleInfo dbIfo = ((DBAssembleManagerImpl) dbManager).getDBAssembleInfo(bucketNo);
                for (DBInfo dbInfoId : dbIfo.getDbInfoMap().values()) {
                    ByteBuffer buffer = dbInfoId.getByteBuffer();
                    int sizePer = mapping.getSIZE_PER(size);
                    int count = mapping.getSIZE_COUNT(size);
                    for (int index = 0; index < count; index++) {
                        int offsetInByte = index * sizePer;
                        byte[] data = new byte[sizePer];
                        synchronized (buffer) {
                            buffer.position(offsetInByte);
                            buffer.get(data, 0, sizePer);
                        }

                        int newKeyLength = Logarithm.bytesToInt(data, 0);
                        int newVersion = Logarithm.bytesToInt(data, 4);
                        int newContentLength = Logarithm.bytesToInt(data, 8);
                        byte newKey[] = Arrays.copyOfRange(data, 12, 12 + newKeyLength);
                        long newExpireTime = Logarithm.getLong(data, newContentLength + 4);

                        MdbPointer point = keyManager.getStoreInfo(newKey, bucketNo);

                        if (point != null) {
                            if (newExpireTime > 0) {
                                if (point.getDbNo().equals(dbInfoId.getDbNo()) && point.getOffset() == offsetInByte && point.getVersion() == newVersion && point
                                        .getBuckId() == bucketNo) {
                                } else {
                                    System.out.println(new String(newKey) + " do not same with the key manager!");
                                }
                            } else {
                                if (point.getDbNo().equals(dbInfoId.getDbNo()) && point.getOffset() == offsetInByte && point.getVersion() == newVersion && point
                                        .getBuckId() == bucketNo) {
                                    System.out.println(new String(newKey) + " should not in the key manager!");
                                }
                            }
                        } else {
                            if (newExpireTime > 0) {
                                System.out.println(new String(newKey) + " could not find in the key manager!  " + dbInfoId.getDbNo());
                                error++;
                            }
                        }
                    }
                }
            }
        }
        System.out.println("verifyBufferForTest end..................................... error ---------------> " + error);
    }

}
