package com.hippo.redis;

import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 *
 */
public interface RedisAdaptor {
	
	public Command set(byte[] key0, byte[] value1);
	
	public Command setex(byte[] key0, byte[] seconds1, byte[] value2);
	
	public Command setbit(byte[] key0, byte[] offset1, byte[] value2);
	
	public Command getbit(byte[] key0, byte[] offset1);
	
	public Command get(byte[] key0);
	
	public Command incr(byte[] key0);
	
	public Command incrby(byte[] key0, byte[] increment1);
	
	public Command decr(byte[] key0);
	
	public Command decrby(byte[] key0, byte[] decrement1);
	
	public Command del(byte[][] key0);
	
	public Command exists(byte[] key0);
	
	public Command ping();
	
	public Command echo(byte[] message0);
	
	
	
}
