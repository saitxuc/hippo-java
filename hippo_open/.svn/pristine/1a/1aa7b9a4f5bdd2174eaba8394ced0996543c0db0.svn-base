package com.pinganfu.hippo.network.transport.nio.coder;

import com.pinganfu.hippo.network.command.Command;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 
 * @author saitxuc
 * 2015-3-4
 */
public interface CoderInitializer {
	
	ByteToMessageDecoder getDecoder();
	
	MessageToByteEncoder<? extends Command> getEncoder();
	
	void close();
}
