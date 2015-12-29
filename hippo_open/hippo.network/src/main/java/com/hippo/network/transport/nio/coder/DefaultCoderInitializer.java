package com.hippo.network.transport.nio.coder;

import java.io.IOException;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 * 2015-4-1
 *
 */
public class DefaultCoderInitializer implements CoderInitializer<Command> {
	
	protected Serializer serializer = null;
	
	public DefaultCoderInitializer() {
		this.serializer = new KryoSerializer();
	}
	
	public DefaultCoderInitializer(Serializer serializer) {
		this.serializer = serializer;
	}
	
	@Override
	public ByteToMessageDecoder getDecoder() {
		return new DefaultSerializerDecoder(1048576 * 20, serializer);
	}

	@Override
	public MessageToByteEncoder<Command> getEncoder() {
		return new DefaultSerializerEncoder(serializer);
	}

	@Override
	public void close() {
		if(serializer != null) {
			serializer.close();
		}
	}
	
	
}
