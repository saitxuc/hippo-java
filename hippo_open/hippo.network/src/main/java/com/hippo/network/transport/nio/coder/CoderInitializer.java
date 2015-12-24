package com.hippo.network.transport.nio.coder;

import com.hippo.network.command.Command;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 
 * @author saitxuc
 * 2015-3-4
 * @param <T>
 */
public interface CoderInitializer<T> {
	
	ByteToMessageDecoder getDecoder();
	
	MessageToByteEncoder<T> getEncoder();
	
	void close();
}
