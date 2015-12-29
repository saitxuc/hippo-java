package com.hippo.client;

import com.hippo.client.impl.HippoClientImpl;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


public class MSClusterPress {
    public static void main(String[] args) {
        String ip1 = args[0];
        String ip2 = args[1];
        int byteLength = Integer.parseInt(args[2]);

        System.out.println("ip -> " + ip1 + " - " + ip2);
        System.out.println("length -> " + byteLength);

        final AtomicLong successCount = new AtomicLong(0L);
        final AtomicLong exceptionCount = new AtomicLong(0L);

        int clientCount = 1;
        int threadCount = 10;

        ExecutorService service = Executors.newFixedThreadPool(threadCount * clientCount);

        final CountDownLatch cd = new CountDownLatch(1);

        final StringBuilder str = new StringBuilder();

        for (int byteNum = 0; byteNum < byteLength; byteNum++) {
            str.append("a");
        }

        for (int j = 0; j < clientCount; j++) {
            HippoConnector hippoConnector = new HippoConnector();
            hippoConnector.setBrokerUrl("failover:(hippo://" + ip1 + ":61000,hippo://" + ip2 + ":61000)");
            hippoConnector.setSessionInstance(threadCount);

            final HippoClientImpl client = new HippoClientImpl(hippoConnector);
            client.start();

            for (int i = 0; i < threadCount; i++) {
                service.execute(new Runnable() {
                    public void run() {
                        try {
                            cd.await();
                            while (true) {
                                try {
                                    String keyString = UUID.randomUUID().toString();
                                    HippoResult result = client.set(6000, keyString, str.toString());
                                    if (result.isSuccess())
                                        successCount.incrementAndGet();
                                    else
                                        exceptionCount.incrementAndGet();
                                } catch (Exception e) {
                                    exceptionCount.incrementAndGet();
                                }
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

        while (true) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            System.out
                .println("exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0D * success2 / (System
                    .currentTimeMillis() - start)) + "]");
        }
    }
}
