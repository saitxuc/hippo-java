package com.hippo.redis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.hippo.client.command.SetCommand;
import com.hippo.common.exception.HippoException;
import com.hippo.network.CommandManager;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.ConnectionInfo;
import com.hippo.network.command.RemoveConnectionCommand;
import com.hippo.network.command.RemoveSessionCommand;
import com.hippo.network.command.Response;
import com.hippo.network.command.SessionInfo;
import com.hippo.network.transport.nio.coder.DefaultCoderInitializer;
import com.hippo.network.transport.nio.server.NioServerDefaultHandler;
import com.hippo.network.transport.nio.server.NioServerInitializer;
import com.hippo.network.transport.nio.server.NioTransportServer;
import com.hippo.redis.command.RedisCommand;
import com.hippo.redis.command.RedisCommandConstants;
import com.hippo.redis.exception.RedisException;
import com.hippo.redis.util.BytesKey;

import static com.hippo.redis.ErrorReply.NYI_REPLY;
import static com.hippo.redis.StatusReply.QUIT;


/**
 * 
 * @author saitxuc
 *
 */
public class RedisTransportServer extends NioTransportServer {
	
	protected static final Logger LOG = LoggerFactory.getLogger(RedisTransportServer.class);
	
	private static final byte LOWER_DIFF = 'a' - 'A';
	
	private Map<BytesKey, Wrapper> methods = new HashMap<BytesKey, Wrapper>();
	
	private Map<BytesKey, ReplyWrapper> replyMethods = new HashMap<BytesKey, ReplyWrapper>();
	
	private RedisAdaptor redisAdaptor;
	
	private ReplyAdaptor replyAdaptor;
	
	public RedisTransportServer(int port, CommandManager<BytesKey> commandManager) {
		super(port, commandManager);
		redisAdaptor = new HippoRedisAdaptor();
		Class<? extends RedisAdaptor> aClass = redisAdaptor.getClass();
		for (final Method method : aClass.getMethods()) {
			final Class<?>[] types = method.getParameterTypes();
			methods.put(new BytesKey(method.getName().getBytes(Charsets.UTF_8)),
					new Wrapper() {
						@Override
						public Command execute(RedisCommand redisCommand) {
							Object[] objects = new Object[types.length];
							try {
								redisCommand.toArguments(objects, types);
								return (Command) method.invoke(redisAdaptor,
										objects);
							} catch (IllegalAccessException e) {
								LOG.error("Invalid server implementation");
								return null;
							} catch (InvocationTargetException e) {
								LOG.error(e.getMessage());
								return null;
							}
						}
					});
		}
		replyAdaptor = new HippoReplyAdaptor();
		Class<? extends ReplyAdaptor> bClass = replyAdaptor.getClass();
		for (final Method method : bClass.getMethods()) {
			final Class<?>[] types = method.getParameterTypes();
			replyMethods.put(new BytesKey(method.getName().getBytes(Charsets.UTF_8)),
			new ReplyWrapper() {

				@Override
				public Reply execute(CommandResult cresult) {
					Object[] objects = new Object[types.length];
					objects[0] = cresult;
					try {
						return (Reply) method.invoke(replyAdaptor,
								objects);
					} catch (IllegalAccessException e) {
						LOG.error("Invalid server implementation");
						return null;
					} catch (InvocationTargetException e) {
						LOG.error(e.getMessage());
						return null;
					}
				}
				
			});
		}

	}
	
	interface Wrapper {
		Command execute(RedisCommand redisCommand);
	}
	
	interface ReplyWrapper {
		Reply execute(CommandResult cresult);
	}
	
	@Override
	public void doInit() {
		if(serverFortress == null) {
			throw new RuntimeException(" TransportServer do not set ServerFortress, cannot do work normally! ");
		}
		if(commandManager == null) {
			throw new RuntimeException(" TransportServer do not set commandManager, cannot do work normally! ");
		}
		coderInitializer = new RedisCoderInitializer();
		
		// Configure the server.
		RedisCommandHandler nethandler = new RedisCommandHandler(this);
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup();
		boot = new ServerBootstrap();
		boot.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_RCVBUF, 1024 * 1024)
				.childOption(
						ChannelOption.RCVBUF_ALLOCATOR,
						new AdaptiveRecvByteBufAllocator(1024, 1024 * 1024,
								10 * 1024 * 1024))
				.childHandler(new NioServerInitializer(nethandler, coderInitializer, false));
				

	}
	
	
	@Override
	public void handleCommand(Object ctx, Command request) throws HippoException {
		if(!(request instanceof RedisCommand)) {
			throw new HippoException("Command is not instance of Redis command.");
		}
		RedisCommand redisCommand = (RedisCommand)request;
		byte[] name = redisCommand.getName();
	    for (int i = 0; i < name.length; i++) {
	      byte b = name[i];
	      if (b >= 'A' && b <= 'Z') {
	        name[i] = (byte) (b + LOWER_DIFF);
	      }
	    }
		BytesKey commandKey = new BytesKey(name);
		ChannelHandlerContext channelHandlerContext = (ChannelHandlerContext)ctx;
		
		if(RedisCommandConstants.REDIS_QUIT_COMMAND.equals(commandKey)) {
			channelHandlerContext.close();
			return;
		}
		
		Command hippoCommand = convertRedisToHippoCommand(redisCommand, commandKey);
		CommandResult cresult = null;
		if(hippoCommand != null) {
			cresult = this.commandManager.handleCommand(hippoCommand, commandKey);
		}
		
		Reply reply = convertHippoResultToReply(cresult, commandKey);
		if (reply == QUIT) {
			channelHandlerContext.close();
		} else {
			if (redisCommand.isInline()) {
				if (reply == null) {
					reply = new InlineReply(null);
				} else {
					reply = new InlineReply(reply.data());
				}
			}
			if (reply == null) {
				reply = NYI_REPLY;
			}
		}
		channelHandlerContext.writeAndFlush(reply);
	}
	
	private Command convertRedisToHippoCommand(RedisCommand redisCommand, BytesKey commandKey) {
		Command hippoCommand  = null;
		Wrapper wrapper = methods.get(commandKey);
		if (wrapper != null) {
	      hippoCommand = wrapper.execute(redisCommand);
	    } 
		
		return hippoCommand;
	}
	
	
	private Reply convertHippoResultToReply(CommandResult cresult, BytesKey commandKey) {
		Reply reply  = null;
		ReplyWrapper wrapper = replyMethods.get(commandKey);
		if (wrapper != null) {
			reply = wrapper.execute(cresult);
	    } 
		
		return reply;
	}
	
}
