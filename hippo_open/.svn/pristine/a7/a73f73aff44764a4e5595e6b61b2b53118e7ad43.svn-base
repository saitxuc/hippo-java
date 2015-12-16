package com.pinganfu.hippo.network.transport.nio.coder;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.network.command.Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 
 * @author saitxuc
 * 2015-4-1
 */
public class DefaultSerializerDecoder extends LengthFieldBasedFrameDecoder {
	
	protected static final Logger LOG = LoggerFactory.getLogger(DefaultSerializerDecoder.class);
	
	
	private Serializer serializer = null;

	public DefaultSerializerDecoder(int maxFrameLength, Serializer serializer) {
		this(maxFrameLength, 0, 4, 0, 4, serializer);
	}

	public DefaultSerializerDecoder(int maxFrameLength, int lengthFieldOffset,
			int lengthFieldLength, int lengthAdjustment,
			int initialBytesToStrip, Serializer serializer) {
		super(maxFrameLength, lengthFieldOffset, lengthFieldLength,
				lengthAdjustment, initialBytesToStrip);
		this.serializer = serializer;
	}
	
	@Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		Object obj = super.decode(ctx, in);
        if (obj == null) {
            return null;
        }
        ByteBuf frame = (ByteBuf) obj;
        try{
        	frame.markReaderIndex();
    		int alllength=	frame.readableBytes();
    		int commandLength = frame.readInt();
    		if(commandLength <= 0) {
    			return null;
    		}
    		byte[] commandbytes = new byte[commandLength];
            frame.readBytes(commandbytes);
            byte[] databytes = null;
            if(alllength - commandLength - 4 > 0) {
            	databytes = new byte[alllength - commandLength - 4];
            	frame.readBytes(databytes);
            }
            Command command = (Command) this.serializer.deserialize(commandbytes, Command.class);
    		if (command == null || StringUtils.isEmpty(command.getAction())) {
    			LOG.warn("===>can not parse the command:"+ command);
    			return null;
    		} else {
    			if(databytes != null && databytes.length > 0) {
    				command.setData(databytes);
    			}
    			return command;
    		}
        }finally {
        	if(frame != null) {
        		frame.release();
        	}
       }
	
        

    }
	
}
