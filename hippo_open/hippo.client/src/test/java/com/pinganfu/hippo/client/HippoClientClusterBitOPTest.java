package com.pinganfu.hippo.client;

import com.pinganfu.hippo.client.impl.HippoClientImpl;
import com.pinganfu.hippo.common.util.DataUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HippoClientClusterBitOPTest {
    public static void main(String[] args) {
        String cclusterName = args[0];
        String zk = args[1];
        final String op = args[2];
        final int beginCount = Integer.parseInt(args[3]);
        final int maxCount = Integer.parseInt(args[4]);
        final String key = args[5];
        final int expire = Integer.parseInt(args[6]);
        final int clientExpire = Integer.parseInt(args[7]);

        System.out.println("cclusterName -> " + cclusterName);
        System.out.println("zookeeper -> " + zk);
        System.out.println("op -> " + op);
        System.out.println("beginCount -> " + beginCount);
        System.out.println("maxCount -> " + maxCount);
        System.out.println("key -> " + key);
        System.out.println("expire -> " + expire);
        System.out.println("clientExpire -> " + clientExpire);

        final AtomicLong successCount = new AtomicLong(0L);
        final AtomicLong exceptionCount = new AtomicLong(0L);

        final int clientCount = 10;
        int threadCount = 10;

        ExecutorService service = Executors.newFixedThreadPool(threadCount * clientCount);

        final AtomicInteger offset = new AtomicInteger(beginCount);

        final CountDownLatch cd = new CountDownLatch(1);

        for (int j = 0; j < clientCount; j++) {
            HippoConnector hippoConnector = new HippoConnector();
            hippoConnector.setClusterName(cclusterName);
            hippoConnector.setZookeeperUrl(zk);
            hippoConnector.setSessionInstance(threadCount);

            final HippoClientImpl client = new HippoClientImpl(hippoConnector);
            client.start();

            for (int i = 0; i < threadCount; i++) {
                service.execute(new Runnable() {
                    public void run() {
                        try {
                            cd.await();
                            while (offset.get() <= maxCount)
                                try {
                                    if (op.equals("set")) {
                                        int off = offset.getAndIncrement();
                                        HippoResult result = client.setBit(key, off, true, expire, clientExpire);
                                        if (result.isSuccess()) {
                                            successCount.incrementAndGet();
                                        } else {
                                            System.out.println("key : " + key + " not be set!! offset -> " + off);
                                            exceptionCount.incrementAndGet();
                                        }
                                    } else if (op.equals("remove")) {
                                        HippoResult result = client.removeWholeBit(key, -1, clientExpire, 60);
                                        if (result.isSuccess()) {
                                            successCount.incrementAndGet();
                                        } else {
                                            exceptionCount.incrementAndGet();
                                        }
                                    } else if (op.equals("get")) {
                                        int off = offset.getAndIncrement();
                                        HippoResult result = client.getBit(key, off, clientExpire);
                                        if (result.isSuccess()) {
                                            byte[] re = result.getData();
                                            if (DataUtil.getBoolean(re[0])) {
                                                successCount.incrementAndGet();
                                            } else {
                                                System.out.println("key : " + key + " not right!! offset -> " + off);
                                                exceptionCount.incrementAndGet();
                                            }
                                        } else {
                                            System.out.println("error code : ->" + result.getErrorCode());
                                            exceptionCount.incrementAndGet();
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    exceptionCount.incrementAndGet();
                                }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }

        cd.countDown();
        long start = System.currentTimeMillis();
        while (offset.get() <= maxCount) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            System.out
                .println("all:[" + offset.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0D * success2 / (System
                    .currentTimeMillis() - start)) + "]");
        }
        System.exit(1);
    }
}
