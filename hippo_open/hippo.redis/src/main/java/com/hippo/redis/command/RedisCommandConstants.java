package com.hippo.redis.command;

import com.google.common.base.Charsets;
import com.hippo.redis.util.BytesKey;

/**
 * 
 * @author saitxuc
 *
 */
public class RedisCommandConstants {
	
	public static final BytesKey REDIS_GET_COMMAND = new BytesKey("get".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_SET_COMMAND = new BytesKey("set".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_SETBIT_COMMAND = new BytesKey("setbit".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_GETBIT_COMMAND = new BytesKey("getbit".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_INCR_COMMAND = new BytesKey("incr".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_INCRBY_COMMAND = new BytesKey("incrby".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_DECR_COMMAND = new BytesKey("decr".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_DECRBY_COMMAND = new BytesKey("decrby".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_DEL_COMMAND = new BytesKey("del".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_EXISTS_COMMAND = new BytesKey("exists".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_AUTH_COMMAND = new BytesKey("AUTH".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_PING_COMMAND = new BytesKey("PING".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_ECHO_COMMAND = new BytesKey("echo".getBytes(Charsets.UTF_8));
	
	public static final BytesKey REDIS_QUIT_COMMAND = new BytesKey("quit".getBytes(Charsets.UTF_8));
	
	
	
}
