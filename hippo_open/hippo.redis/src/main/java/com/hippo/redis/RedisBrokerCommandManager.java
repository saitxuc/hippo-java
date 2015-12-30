package com.hippo.redis;

import com.hippo.network.BaseCommandManager;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.EchoCommand;
import com.hippo.network.command.PingCommand;
import com.hippo.network.transport.nio.CommandHandle;
import com.hippo.redis.command.RedisCommand;
import com.hippo.redis.command.RedisCommandConstants;
import com.hippo.redis.command.handler.RedisOpCommandHandler;
import com.hippo.redis.util.BytesKey;

/**
 * 
 * @author saitxuc
 *
 */
public class RedisBrokerCommandManager extends BaseCommandManager<BytesKey> {
	
	private CommandHandle realHandler = null;
	
	public RedisBrokerCommandManager() {
		super();
	}
	
	public RedisBrokerCommandManager(CommandHandle realHandler) {
		this.realHandler = realHandler;
		this.init();
	}
	
	@Override
	public void initConmandHandler() {
		addCommandHandler(RedisCommandConstants.REDIS_SET_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_GET_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_SETBIT_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_GETBIT_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_INCR_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_INCRBY_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_DECR_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_DECRBY_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_DEL_COMMAND, new RedisOpCommandHandler(realHandler));
		addCommandHandler(RedisCommandConstants.REDIS_EXISTS_COMMAND, new RedisOpCommandHandler(realHandler));
	}
	
	@Override
	public CommandResult handleCommand(Command command, BytesKey commandKey) {
		
		if(PingCommand.PING_ACTION.equals(command.getAction())) {
			return ping();
		}
		if(EchoCommand.ECHO_ACTION.equals(command.getAction())) {
			return echo(command.getData());
		}
		CommandHandle commandHandle = this.getCommandHandler(commandKey);
		CommandResult cresult = null;
		if(commandHandle != null) {
			try {
				cresult = commandHandle.doCommand(command);
			} catch (Exception e) {
				LOG.error(" handleCommand happen error。 ", e);
			}
		}
		return cresult;
	}
	
	public void setRealHandler(CommandHandle realHandler) {
		this.realHandler = realHandler;
	}
	
	private CommandResult ping() {
		CommandResult cresult = new CommandResult(true);
		return cresult;
	}
	
	private CommandResult echo(byte[] messages) {
		CommandResult cresult = new CommandResult(true);
		cresult.setData(messages);
		return cresult;
	}
	
}
