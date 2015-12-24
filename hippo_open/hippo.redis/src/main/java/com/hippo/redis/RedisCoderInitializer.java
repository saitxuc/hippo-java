package com.hippo.redis;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import com.hippo.network.transport.nio.coder.CoderInitializer;
import com.hippo.redis.coder.RedisCommandDecoder;
import com.hippo.redis.coder.RedisReplyEncoder;

/**
 * 
 * @author saitxuc
 *
 */
public class RedisCoderInitializer implements CoderInitializer<Reply> {

	@Override
	public ByteToMessageDecoder getDecoder() {
		return new RedisCommandDecoder();
	}

	@Override
	public MessageToByteEncoder<Reply> getEncoder() {
		return new RedisReplyEncoder();
	}

	@Override
	public void close() {
		
	}

}
