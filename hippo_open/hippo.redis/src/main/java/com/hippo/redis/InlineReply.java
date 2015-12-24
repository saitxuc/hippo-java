package com.hippo.redis;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Charsets;

import static com.hippo.redis.util.Encoding.numToBytes;

/**
 * 
 * @author saitxuc
 *
 */
public class InlineReply implements Reply<Object> {

	  private final Object o;

	  public InlineReply(Object o) {
	    this.o = o;
	  }

	  @Override
	  public Object data() {
	    return o;
	  }

	  @Override
	  public void write(ByteBuf os) throws IOException {
	    if (o == null) {
	      os.writeBytes(CRLF);
	    } else if (o instanceof String) {
	      os.writeByte('+');
	      os.writeBytes(((String) o).getBytes(Charsets.US_ASCII));
	      os.writeBytes(CRLF);
	    } else if (o instanceof ByteBuf) {
	      os.writeByte('+');
	      os.writeBytes(((ByteBuf) o).array());
	      os.writeBytes(CRLF);
	    } else if (o instanceof byte[]) {
	      os.writeByte('+');
	      os.writeBytes((byte[]) o);
	      os.writeBytes(CRLF);
	    } else if (o instanceof Long) {
	      os.writeByte(':');
	      os.writeBytes(numToBytes((Long) o, true));
	    } else {
	      os.writeBytes("ERR invalid inline response".getBytes(Charsets.US_ASCII));
	      os.writeBytes(CRLF);
	    }
	  }
	}
