package com.test.mdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.mdb.MdbStoreEngine;

/**
 * 
 * @author saitxuc
 * write 2014-7-30
 */
public class MdbStoreEngineCoverPerfTest {

    public static void main(final String[] args) throws Exception {

        final AtomicLong successCount = new AtomicLong(0);
        final AtomicLong exceptionCount = new AtomicLong(0);
        final byte[] msgContent = ("message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。" + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格" + "式的更多描述。我们来具体研究一下文件")
            .getBytes();

        System.out.println(msgContent.length);
        //        final String msgContent = "abcdefghijklmnopqrstuvwxyz";
        //    final byte[] mcontent = new byte[900];
        final AtomicLong allCount = new AtomicLong(0);
        int threadCount = 1;
        final int maxCount = 1024 * 1024 * 5;//10000000
        ExecutorService service = Executors.newFixedThreadPool(threadCount);

        final Random ran = new Random();

        List<BucketInfo> list = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, true);
        list.add(info);

        final MdbStoreEngine engine = new MdbStoreEngine();
        engine.setLimit(500L * 1024L * 1024L);
        engine.setBuckets(list);
        engine.setBucketLimit(2);
        engine.start();
        //engine.startPeriodsExpire();

        final CountDownLatch cd = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        cd.await();
                        for (int i = 0; i <= 1000000; i++) {
                            boolean succful = engine.addData((i + "").getBytes(), msgContent, 20000, 0);
                            if (succful) {
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

        for (int i = 5; i < threadCount; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (; ;) {
                            for (int i = 0; i <= 1000000; i++) {
                                boolean succful = engine.addData((i + "").getBytes(), msgContent, 20000, 0);
                                if (succful) {
                                    successCount.incrementAndGet();
                                } else {
                                    exceptionCount.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptionCount.incrementAndGet();
                    }
                }
            });
        }

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
            if (allCount.get() > 10000000) {
                System.out
                    .println("LRU ----> param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                        .currentTimeMillis() - start)) + "],size->" + engine.size() + ",capacity->" + engine.getCurrentUsedCapacity() + ",bucketCapacity->" + engine
                        .getBucketUsedCapacity(0));
            } else {
                System.out
                    .println("param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                        .currentTimeMillis() - start)) + "],size->" + engine.size() + ",capacity->" + engine.getCurrentUsedCapacity() + ",bucketCapacity->" + engine
                        .getBucketUsedCapacity(0));
            }
        }

        Thread.sleep(1000000L);
        service.shutdown();
        System.out.println("----DB--count------->>>>>>>" + engine.getDataCount());

        engine.stop();
        System.exit(1);
    }

}
