package com.test.mdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.hippo.common.domain.BucketInfo;
import com.hippo.common.util.ExcutorUtils;
import com.hippo.mdb.MdbStoreEngine;
import com.hippo.store.model.GetResult;

public class MdbStoreEngineAddValidation {
    public static void main(String[] args) {
        final MdbStoreEngine storeEngine = new MdbStoreEngine();
        storeEngine.setLimit(1L * 1024L * 1024L * 1024L);
        storeEngine.start();
        storeEngine.startPeriodsExpire();

        ExecutorService aa = ExcutorUtils.startSingleExcutor("test");
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 1; i <= 50; i++) {
                        for (int count = 1; count <= 10000; count++) {
                            String key = "k" + (count + (10000 * (i - 1)));
                            boolean result = storeEngine.addData(key.getBytes(), key.getBytes(), 3000,0);
                            if (!result) {
                                System.out.println("data added expcetion!");
                            }
                        }

                        for (int count = 1; count <= 10000; count++) {
                            String key = "k" + (count + (5000 * (i - 1)));
                            boolean result2 = storeEngine.addData(key.getBytes(), key.getBytes(), 3000,0);
                            if (!result2) {
                                System.out.println("data added expcetion!");
                            }
                        }

                        //Thread.sleep(5000);
                    }
                    System.out.println("size is ----------------" + storeEngine.size());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("test begin");
                for (int i = 1; i <= 500000; i++) {
                    String key = "k" + i;
                    try {
                        GetResult result = storeEngine.getData(key.getBytes(),0);
                        if (!key.equals(new String(result.getContent()))) {
                            System.out.println("key value not the same , the key is " + key + ",the value is " + new String(result.getContent()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("test end");
            }
        });
        aa.execute(t);
        System.exit(1);
    }
}
