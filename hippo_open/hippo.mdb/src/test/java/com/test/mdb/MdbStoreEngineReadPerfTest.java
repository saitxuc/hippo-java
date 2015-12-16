package com.test.mdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.store.StoreEngine;
import com.pinganfu.hippo.store.model.GetResult;

/**
 * 
 * @author saitxuc
 * 2015-1-26
 */
public class MdbStoreEngineReadPerfTest {
    public static void main(final String[] args) throws Exception {

        final AtomicLong successCount = new AtomicLong(0);
        final AtomicLong exceptionCount = new AtomicLong(0);
        /***
        final String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
                + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
                + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
                + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
                + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
                + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
                + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
                + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，";
        ***/
        //final String msgContent = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
        final byte[] msgContent = new byte[800];
        final AtomicLong allCount = new AtomicLong(0);
        int threadCount = 10;
        final int maxCount = 1048576;//10000000
        ExecutorService service = Executors.newFixedThreadPool(threadCount);

        List<BucketInfo> list = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, false);
        list.add(info);

        final StoreEngine engine = new MdbStoreEngine();
        engine.setLimit(1024L * 1024L * 1024L);
        engine.setBuckets(list);
        engine.start();

        Serializer serializer = new KryoSerializer();

        final AtomicLong key = new AtomicLong(-1);
        final CountDownLatch cd = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    while (key.get() < maxCount - 2) {
                        boolean succful = engine.addData(("k" + key.incrementAndGet()).getBytes(), msgContent, 2000,0);

                        if (!succful) {
                            System.out.println("data added expcetion!");
                            exceptionCount.incrementAndGet();
                        }
                    }
                    cd.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    //	exceptionCount.incrementAndGet();
                }
            }
        });
        t.start();

        cd.await();

        System.out.println("----key--count------->>>>>>>" + key.get());

        System.out.println("----DB--count------->>>>>>>" + engine.getDataCount());

        System.out.println(engine.size());

        //System.out.println("----key--count------->>>>>>>"+key.get());
        //System.out.println("----key--exceptionCount------->>>>>>>"+exceptionCount.get());

        for (int i = 0; i < threadCount; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Random random = new Random();
                            String key1 = "k" + String.valueOf(random.nextInt(maxCount - 1));
                            //String key1 = "k599999";
                            GetResult result = engine.getData(key1.getBytes(),0);
                            if (result.getContent() != null) {
                                //System.out.println(new String(succful));
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

        long start = System.currentTimeMillis();
        while (allCount.get() <= maxCount) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();

            String str = "use the default param: ojbect--8111--set--10--1--1000000";
            if (allCount.get() > 1000000) {
                System.out
                    .println("LRU ----> param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                        .currentTimeMillis() - start)) + "]");
            } else {
                System.out
                    .println("param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System
                        .currentTimeMillis() - start)) + "]");
            }

        }

        System.out.println("----DB--count------->>>>>>>" + engine.getDataCount());

        engine.stop();
    }

}
