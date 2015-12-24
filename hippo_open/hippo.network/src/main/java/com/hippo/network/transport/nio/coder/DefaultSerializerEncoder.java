package com.hippo.network.transport.nio.coder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.hippo.common.serializer.Serializer;
import com.hippo.network.command.Command;

/**
 * 
 * @author saitxuc
 * 2015-4-1
 */
public class DefaultSerializerEncoder extends MessageToByteEncoder<Command> {
	
	protected static final Logger LOG = LoggerFactory.getLogger(DefaultSerializerEncoder.class);
	
private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
	
	private Serializer serializer = null;
	
	public DefaultSerializerEncoder(Serializer serializer) {
		this.serializer = serializer;
	}
	
	@Override
	protected void encode(ChannelHandlerContext ctx, Command command,
			ByteBuf out) throws Exception {
		try{
        	int startIdx = out.writerIndex();
        	if (command == null) {
				return;
			}
        	ByteBufOutputStream bout = new ByteBufOutputStream(out);
        	byte[] commandBytes = serializer.serialize(command);
        	bout.write(LENGTH_PLACEHOLDER);
        	bout.writeInt(commandBytes.length);
			bout.write(commandBytes);
			if(command.getData() != null 
					&& command.getData().length > 0) {
				bout.write(command.getData());
			}
        	bout.flush();
			bout.close();
			int endIdx = out.writerIndex();
			out.setInt(startIdx, endIdx - startIdx - 4);
    	}catch(Exception e) {
    		LOG.error(e.getMessage());
    		throw e;
    	}
	}
	
}
