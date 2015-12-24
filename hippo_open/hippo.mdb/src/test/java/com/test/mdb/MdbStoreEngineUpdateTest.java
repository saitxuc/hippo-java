package com.test.mdb;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.hippo.common.domain.BucketInfo;
import com.hippo.mdb.MdbStoreEngine;
import com.hippo.store.StoreEngine;
import com.hippo.store.model.GetResult;

/**
 * 
 * @author saitxuc
 * write 2014-7-30
 */
public class MdbStoreEngineUpdateTest {

    public static void main(final String[] args) throws Exception {
        List<BucketInfo> buckets = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, false);
        buckets.add(info);
        
        final MdbStoreEngine engine = new MdbStoreEngine();
        engine.setLimit(1073741824);
        engine.setBuckets(buckets);
        engine.setBucketLimit(2);
        engine.start();

        final AtomicLong successCount = new AtomicLong(0);
        final AtomicLong exceptionCount = new AtomicLong(0);

        final String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。" + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格" + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示" + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码" + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，" + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi " + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) 编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r)编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r)编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r)" + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2";

        byte[] key = "test1".getBytes();
        byte[] val1 = "123123".getBytes();
        byte[] val2 = "1234567890123456789".getBytes();
        byte[] val3 = msgContent.getBytes();

        //   System.out.println("val3 ->" + val3.length);

        engine.addData(key, val1, 2000, 0, 0);

        GetResult result = engine.getData(key, 0);

        System.out.println("before modify->" + new String(result.getContent()) + " version -> " + result.getVersion());

        engine.updateData(key, val2, 2000, 0, 0);

        result = engine.getData(key, 0);
        System.out.println("after modify->" + new String(result.getContent()) + " version -> " + result.getVersion());

        engine.updateData(key, val3, 2000, 0, 0);

        result = engine.getData(key, 0);
        System.out.println("after modify->" + new String(result.getContent()) + " version -> " + result.getVersion());

        engine.updateData(key, val2, 2000, 0, 0);
        result = engine.getData(key, 0);
        System.out.println("after modify->" + new String(result.getContent()) + " version -> " + result.getVersion());

        //engine.removeData(key, 0, 2);

        /*int i = 0;
        while (i < 50000) {
            boolean succful = engine.addData((i + "").getBytes(), new byte[500], 2000, 0);

            if (!succful) {
                System.out.println("data added expcetion!");
                exceptionCount.incrementAndGet();
            }
            i++;
        }
        */
        //engine.updateData(key, val3, 2000, 0);

        //  System.out.println("after modify->" + new String(result.getContent()) + " version -> " + result.getVersion());

        System.out.println("size->" + engine.size());

        /* for (int j = 0; j < 1000000; j++) {
             if(!engine.containKey((j + "").getBytes())){
                 System.out.println(j);
             }
         }*/

        /*  i = 0;
          while (i < 10000) {
              if (!engine.removeData((i + "").getBytes())) {
                  System.out.println("remove failed!!");
              }
              i++;
          }
          System.out.println(engine.size());*/
        // System.out.println("after modify->" + new String(engine.getData(key)));

        // System.out.println(new String(engine.getData(key)));

        engine.stop();
        // System.exit(1);
    }
}
