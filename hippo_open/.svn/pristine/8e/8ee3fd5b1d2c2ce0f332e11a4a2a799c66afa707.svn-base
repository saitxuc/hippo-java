package com.pinganfu.hippo.client.transport.netty.client;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.HessionSerializer;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.Session;
import com.pinganfu.hippo.network.command.EchoCommand;
import com.pinganfu.hippo.network.impl.TransportConnectionFactory;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class HippoClientPerfTest {
	
	private static String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，";
	
	public static void main(final String[] args) throws Exception {
		final AtomicLong successCount = new AtomicLong(0);
		final AtomicLong exceptionCount = new AtomicLong(0);
		msgContent = "abcdefghijklmnopqrstuvwxyz";
		final AtomicLong allCount = new AtomicLong(0);
		int clientCount = 10;
		int threadCount = 20;
		final int maxCount = 10000000;//10000000
		ExecutorService service = Executors.newFixedThreadPool(threadCount);
		final CountDownLatch cd = new CountDownLatch(1);
		for(int j = 0; j < clientCount; j++) {
			ConnectionFactory connectionFactory = new TransportConnectionFactory("sait", "sait", "Netty://127.0.0.1:61300");
			//connectionFactory.setCommandManager(new ClientCommandManager());
			//connectionFactory.setSerializer(new KryoSerializer());
			
			Connection connection = connectionFactory.createConnection();
			connection.start();
			for (int i = 0; i < threadCount; i++) {
				final Session session = connection.createSession();
				session.start();
				//final CacheProvider cacheProvider = session.createProvider();
				service.execute(new Runnable() {
					@Override
					public void run() {
						try {
							cd.await();
							while (allCount.incrementAndGet() <= maxCount) {
								EchoCommand command = new EchoCommand();
								command.setContent(msgContent);
								try{
									Object rsp = session.send(command, 1000);
									successCount.incrementAndGet();
								}catch(Exception e) {
									exceptionCount.incrementAndGet();
								}
								
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
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

			String str =  "use the default param: ojbect--8111--set--10--1--1000000";
			if (allCount.get() > 1000000) {
				System.out.println("LRU ----> param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System.currentTimeMillis() - start)) + "]");
			} else {
				System.out.println("param: " + str + "all:[" + allCount.get() + "]exceptioncount:[" + exceptionCount.get() + "] per second :[" + (success2 - success1) + "]tps :[" + (int) (1000.0 * success2 / (System.currentTimeMillis() - start)) + "]");
			}

		}
	}
	
}
