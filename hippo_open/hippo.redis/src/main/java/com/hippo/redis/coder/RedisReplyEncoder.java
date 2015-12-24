package com.hippo.redis.coder;

import com.hippo.redis.Reply;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 
 * @author saitxuc
 * 
 */
public class RedisReplyEncoder extends MessageToByteEncoder<Reply> {
	
	  @Override
	  public void encode(ChannelHandlerContext ctx, Reply msg, ByteBuf out) throws Exception {
	    msg.write(out);
	  }
	
}
