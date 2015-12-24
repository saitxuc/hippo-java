package com.hippo.redis;

import com.hippo.network.CommandResult;

/**
 * 
 * @author saitxuc
 *
 */
public interface ReplyAdaptor {
	
	public Reply set(CommandResult cresult);
	
	public Reply setex(CommandResult cresult);
	
	public Reply setbit(CommandResult cresult);
	
	public Reply getbit(CommandResult cresult);
	
	public Reply get(CommandResult cresult);
	
	public Reply incr(CommandResult cresult);
	
	public Reply incrby(CommandResult cresult);
	
	public Reply decr(CommandResult cresult);
	
	public Reply decrby(CommandResult cresult);
	
	public Reply del(CommandResult cresult);
	
	public Reply exists(CommandResult cresult);
	
	public Reply ping(CommandResult cresult);
	
	public Reply echo(CommandResult cresult);
}
