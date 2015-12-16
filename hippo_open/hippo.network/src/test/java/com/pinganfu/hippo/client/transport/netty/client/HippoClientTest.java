package com.pinganfu.hippo.client.transport.netty.client;

import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.Session;
import com.pinganfu.hippo.network.command.EchoCommand;
import com.pinganfu.hippo.network.impl.TransportConnectionFactory;

/**
 * 
 * @author saitxuc
 *  write 2014-7-2
 *
 */
public class HippoClientTest {
	
	private static String msgContent = "message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，message body,FD 和 Type 列的含义最为模糊，它们提供了关于文件如何使用的更多信息。"
            + "FD 列表示文件描述符，应用程序通过文件描述符识别该文件。Type 列提供了关于文件格"
            + "式的更多描述。我们来具体研究一下文件描述符列，清单 1 中出现了三种不同的值。cwd 值表示"
            + "应用程序的当前工作目录，这是该应用程序启动的目录，除非它本身对这个目录进行更改。txt 类型的文件是程序代码"
            + "，如应用程序二进制文件本身或共享库，再比如本示例的列表中显示的 init 程序。最后，数值表示应用程序的文件描述符，"
            + "这是打开该文件时返回的一个整数。在清单 1 输出的最后一行中，您可以看到用户正在使用 vi "
            + "编辑 /var/tmp/ExXDaO7d，其文件描述符为 3。u 表示该文件被打开并处于读取/写入模式，而不是只读 (r) "
            + "或只写 (w) 模式。有一点不是很重要但却很有帮助，初始打开每个应用程序时，都具有三个文件描述符，从 0 到 2，";
	
	public static void main(final String[] args) throws Exception {
		
		ConnectionFactory connectionFactory = new TransportConnectionFactory("sait", "sait", "recovery://127.0.0.1:61300");
		connectionFactory.setCommandManager(new ClientCommandManager());
		//connectionFactory.setSerializer(new KryoSerializer());
		Connection connection = connectionFactory.createConnection();
		
		connection.start();
		
		Session session = connection.createSession();
		
		
		//CacheProvider cacheProvider = session.createProvider();
		
		///Thread.sleep(100000);
		
		for(int i = 0; i < 6; i++) {
			EchoCommand command = new EchoCommand();
			command.setContent(msgContent);
			session.asnysend(command);
			///System.out.println("-----cacheProvider-------send------------>>");
		}
		
		
		//session.stop();
		Thread.sleep(30000);
		EchoCommand command = new EchoCommand();
		command.setContent(msgContent);
		session.asnysend(command);
		System.out.println("-----cacheProvider-------send----ssssss-------->>");
		//connection.stop();
		//command = new EchoCommand();
		//command.setContent(msgContent);
		//Object rsp = cacheProvider.send(command, 3000);
		//System.out.println("-----cacheProvider-------rsp------------>>"+rsp);
	}
	
}
