package com.hippo.client;

import com.hippo.client.impl.HippoClientImpl;

public class SimpleHippoClientBitPerfTest {

    public static void main(final String[] args) throws Exception {
        final String ip = "10.59.2.112";
        final String key = "test2";

        final HippoConnector hippoConnector = new HippoConnector();
        hippoConnector.setBrokerUrl("failover:(hippo://" + ip + ":61000)");
        hippoConnector.setSessionInstance(5);
        final HippoClientImpl client = new HippoClientImpl(hippoConnector);
        client.start();

        System.out.println("begin to set");
        for (int i = 0; i < 1500; i++) {
            HippoResult result = client.setBit(key, i, true, 30000, 5);
            if (!result.isSuccess()) {
                System.out.println("set bit " + i + " error, code -> " + result.getErrorCode());
            }
        }

        System.out.println("begin to get");

        for (int i = 0; i < 1500; i++) {
            HippoResult result = client.getBit(key, i, 50);
            if (!result.isSuccess()) {
                System.out.println("get bit " + i + " error, code -> " + result.getErrorCode());
            } else {
                if (result.getData()[0] != 1) {
                    System.out.println("validation not right! offset " + i);
                }
            }
        }

        HippoResult result = client.getWholeBit(key, -1, 500, 100);

        if (result.isSuccess()) {
            byte[] data = result.getData();
            for (int i = 0; i <= data.length; i++) {
                if (data[i] != -1) {
                    System.out.println(i + " not right ! value is " + data[i]);
                    break;
                }
            }
        }

        result = client.removeWholeBit(key, 10000, 500, 100);
        if (result.isSuccess()) {
            System.out.println("remove success");
        }

        System.out.println("begin to get");

        for (int i = 0; i < 100; i++) {
            result = client.getBit(key, i, 50);
            if (!result.isSuccess()) {
                System.out.println("get bit " + i + " error, code -> " + result.getErrorCode());
            } else {
                if (result.getData()[0] != 1) {
                    System.out.println("validation not right! offset " + i);
                }
            }
        }


        client.stop();
        System.exit(1);




       /* for (int j = 0; j < clientCount; j++) {
            final HippoConnector hippoConnector = new HippoConnector();
            hippoConnector.setBrokerUrl("failover:(hippo://" + ip + ":61000)");
            hippoConnector.setSessionInstance(threadCount);
            final HippoClientImpl client = new HippoClientImpl(hippoConnector);
            client.start();

            for (int i = 0; i < threadCount; i++) {
                service.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cd.await();

                            int offset = 0;
                            while (offset <= maxCount) {
                                offset = allCount.incrementAndGet();

                                HippoResult result = client.setBit(key, offset, true, 10000, 5000);

                                if (result.isSuccess()) {
                                    //DataUtil.getBoolean(result.getData()[0]);
                                    successCount.incrementAndGet();
                                } else {
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
        while (allCount.get() <= maxCount) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            System.out
                .println("all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                    .currentTimeMillis() - start)) + "]");
        }
*/
        // System.exit(1);
    }
}
