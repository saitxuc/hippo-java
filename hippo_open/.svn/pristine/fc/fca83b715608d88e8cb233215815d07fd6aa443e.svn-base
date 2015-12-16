package com.test.mdb;

import java.util.concurrent.ExecutorService;
import com.pinganfu.hippo.common.util.ExcutorUtils;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.store.model.GetResult;

public class MdbStoreEngineAddUpdateLargeValidation {
    public static void main(String[] args) {
        final MdbStoreEngine storeEngine = new MdbStoreEngine();
        storeEngine.setLimit(2L * 1024L * 1024L * 1024L);
        storeEngine.start();
        storeEngine.startPeriodsExpire();
        
        final String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格" 
        + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示" + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
                + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
        + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
                + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) 编辑 /var/tmp/(r)编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r)编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r)" + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2";

        System.out.println(msgContent.getBytes().length);
        ExecutorService aa = ExcutorUtils.startSingleExcutor("test");
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 1; i <= 50; i++) {
                        for (int count = 1; count <= 10000; count++) {
                            String key = "k" + (count + (10000 * (i - 1)));
                            boolean result = storeEngine.addData(key.getBytes(), key.getBytes(), 3000 , 0);
                            if (!result) {
                                System.out.println("data added expcetion!");
                            }
                        }
                        System.out.println("update ---" +i);
                        for (int count = 1; count <= 10000; count++) {
                            String key = "k" + (count + (10000 * (i - 1)));
                            String key2 = "k" + (count + (10000 * (i - 1))) + msgContent;
                            boolean result2 = storeEngine.updateData(key.getBytes(), key2.getBytes(), 10000,0);
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
                        if (!(key + msgContent).equals(new String(result.getContent()))) {
                            System.out.println("key value not the same , the key is " + key + ",the value is " + new String(result.getContent()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("test end");
                System.out.println(storeEngine.size());
            }
        });
        aa.execute(t);
    }
}
