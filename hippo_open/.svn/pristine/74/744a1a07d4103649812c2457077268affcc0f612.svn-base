package com.test.mdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.store.StoreEngine;
import com.pinganfu.hippo.store.exception.HippoStoreException;

public class MdbTest {

    public static void main(String[] args) {
        final int count = 100000;
        List<BucketInfo> list2 = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, false);
        list2.add(info);

        final StoreEngine engine = new MdbStoreEngine();
        engine.setBuckets(list2);
        engine.start();
        
        final CountDownLatch cd = new CountDownLatch(1);

        engine.start();

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i <= count; i++) {
                    byte[] obj = (i + "").getBytes();
                    try {
                        engine.addData((i + "").getBytes(), obj, 10000000,1);
                    } catch (HippoStoreException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("(0 - " + count + ")存储完毕");
                cd.countDown();
                Random r = new Random();
                int countTotal = count;
                while (true) {
                    byte[] val = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx x xxxxxxx x xxxxxxxxxxxxx xxxxxxxxxxx".getBytes();
                    countTotal++;
                    //					if(countTotal%1000 == 0)
                    //						System.out.println(countTotal);
                    try {
                        engine.addData(("" + r.nextInt(count)).getBytes(), val, 10000000,1);
                    } catch (HippoStoreException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        final List<String> list = new ArrayList<String>();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                int failcount = 0;
                int succount = 0;
                Random random = new Random();
                while (true) {
                    try {
                        cd.await();
                        String key = random.nextInt(count) + "";
                        //						TestObject obj = (TestObject)engine.getData(key);
                        Object object = engine.getData(key.getBytes(),1);
                        TestObject obj = null;
                        if (object instanceof TestObject) {
                            obj = (TestObject) object;
                        } else {
                            System.out.println(object);
                        }
                        if (obj == null) {
                            failcount++;
                            list.add(key);
                        } else {
                            succount++;
                        }
                        if ((failcount + succount) % 1000 == 0) {
                            System.out.println(obj + ",suc:" + succount + ",fail:" + failcount);
                            if (failcount > 0) {
                                String failStr = "failStr:";
                                for (String str : list) {
                                    failStr += str + ",";
                                }
                                System.out.println(failStr);
                            }
                        }
                    } catch (HippoStoreException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                }
            }
        });

        t1.start();
        t2.start();
    }

}
