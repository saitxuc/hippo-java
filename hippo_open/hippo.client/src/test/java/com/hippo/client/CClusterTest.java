package com.hippo.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.hippo.client.impl.HippoClientImpl;

public class CClusterTest {
    public static void main(String[] args) {
        String cclusterName = args[0];
        String zk = args[1];
        final String op = args[2];
        int beginCount = Integer.parseInt(args[3]);
        final int maxCount = Integer.parseInt(args[4]);
        int byteLength = Integer.parseInt(args[5]);
        final int version = Integer.parseInt(args[6]);

        System.out.println("cclusterName -> " + cclusterName);
        System.out.println("zookeeper -> " + zk);
        System.out.println("op -> " + op);
        System.out.println("beginCount -> " + beginCount);
        System.out.println("maxCount -> " + maxCount);
        System.out.println("length -> " + byteLength);

        final AtomicLong successCount = new AtomicLong(0L);
        final AtomicLong exceptionCount = new AtomicLong(0L);

        int clientCount = 10;
        int threadCount = 10;

        ExecutorService service = Executors.newFixedThreadPool(threadCount * clientCount);

        final AtomicLong key = new AtomicLong(beginCount);

        final CountDownLatch cd = new CountDownLatch(1);

        final StringBuilder str = new StringBuilder();

        for (int byteNum = 0; byteNum < byteLength; byteNum++) {
            str.append("a");
        }

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
                            while (key.get() <= maxCount)
                                try {
                                    String keyString = "key" + key.incrementAndGet();
                                    if (op.equals("set")) {
                                        HippoResult result = client.set(6000, keyString, str.toString(), version);
                                        if (result.isSuccess())
                                            successCount.incrementAndGet();
                                        else
                                            exceptionCount.incrementAndGet();
                                    } else if (op.equals("update")) {
                                        HippoResult result = client.update(6000, keyString, str.toString(), version);
                                        if (result.isSuccess())
                                            successCount.incrementAndGet();
                                        else
                                            exceptionCount.incrementAndGet();
                                    } else if (op.equals("remove")) {
                                        HippoResult result = client.remove(keyString);
                                        if (result.isSuccess())
                                            successCount.incrementAndGet();
                                        else
                                            exceptionCount.incrementAndGet();
                                    } else if (op.equals("incr")) {
                                        String testIncrKey = "testIncrKey";
                                        HippoResult result = client.incr(testIncrKey);
                                        if (result.isSuccess()) {
                                            successCount.incrementAndGet();
                                        } else {
                                            exceptionCount.incrementAndGet();
                                        }
                                    } else if (op.equals("decr")) {
                                        String testDecrKey = "testDecrKey";
                                        HippoResult result = client.decr(testDecrKey);
                                        if (result.isSuccess()) {
                                            successCount.incrementAndGet();
                                        } else {
                                            exceptionCount.incrementAndGet();
                                        }
                                    } else if (op.equals("get")) {
                                        HippoResult result = client.get(keyString);
                                        if (result.isSuccess()) {
                                            String content = (String) result.getDataForObject(String.class);
                                            if (str.toString().equals(content)) {
                                                String ver = (String) result.getAttrMap().get("version");
                                                if (version == Integer.parseInt(ver))
                                                    successCount.incrementAndGet();
                                                else
                                                    exceptionCount.incrementAndGet();
                                            } else {
                                                exceptionCount.incrementAndGet();
                                            }
                                        } else {
                                            exceptionCount.incrementAndGet();
                                        }
                                    }
                                } catch (Exception e) {
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
        while (key.get() <= maxCount) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            System.out
                .println("all:[" + key.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0D * success2 / (System
                    .currentTimeMillis() - start)) + "]");
        }
        System.exit(1);
    }
}
