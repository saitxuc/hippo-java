package com.test.mdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.pinganfu.hippo.common.domain.BucketInfo;
import com.pinganfu.hippo.mdb.MdbStoreEngine;
import com.pinganfu.hippo.store.StoreEngine;
import com.pinganfu.hippo.store.model.GetResult;

/**
 * 
 * @author saitxuc
 * write 2014-7-30
 */
public class MdbStoreEngineTest {

    private static String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。" + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格" + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示" + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码" + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，" + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi " + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) " + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，";

    public static void main(final String[] args) throws Exception {
        List<BucketInfo> list = new ArrayList<BucketInfo>();
        BucketInfo info = new BucketInfo(0, false);
        list.add(info);
        final StoreEngine engine = new MdbStoreEngine();

        engine.setBuckets(list);

        engine.start();
        msgContent = "abcdefghijklmnopqrstuvwxyz12345678";

        //for(int i = 0; i < 2000; i++) {
        boolean succful = engine.addData("test0".getBytes(), msgContent.getBytes(), 10, 0);
        //}

        /***	
        msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
                + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
                + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
                + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
                + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
                + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
                + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
                + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，";	
        ***/
        //boolean succful = engine.addData("test1", msgContent, 10);
        engine.addData("test1".getBytes(), msgContent.getBytes(), 10, 0);
        //msgContent = "abcdefghijklmnopqrstuvwxyz";	

        System.out.println("----------succful---1--->>" + succful);

        //for(int i = 0; i < 2000; i++){
        //succful = engine.updateData("test0", msgContent);
        //	System.out.println("----------succful---2--->>"+succful);
        //}

        GetResult result = engine.getData("test0".getBytes(), 0);
        System.out.println("----------content------>>" + result.getContent());
        result = engine.getData("test1".getBytes(), 0);
        System.out.println("----------content2------>>" + result.getContent());

        //engine.removeData("test0");

        //content = engine.getData("test0");	
        //System.out.println("----------content------>>"+content);

        engine.stop();
    }
}
