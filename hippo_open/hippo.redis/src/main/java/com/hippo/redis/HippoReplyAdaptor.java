package com.hippo.redis;

import java.io.IOException;

import com.hippo.client.ClientConstants;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandResult;

import static com.hippo.redis.IntegerReply.integer;
import static com.hippo.redis.util.Encoding.hippoByteToBoolean;

/**
 * 
 * @author saitxuc
 *
 */
public class HippoReplyAdaptor implements ReplyAdaptor {
	
	private static final StatusReply PONG = new StatusReply("PONG");
	
	@Override
	public Reply set(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				reply = StatusReply.OK;
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}

	@Override
	public Reply setex(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				reply = StatusReply.OK;
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}

	@Override
	public Reply setbit(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				reply = integer(0);
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}

	@Override
	public Reply get(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				reply = new BulkReply(cresult.getData());
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}

	@Override
	public Reply getbit(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				
				byte[] bitB = cresult.getData();
				boolean bitboolean = hippoByteToBoolean(bitB);
				if(bitboolean) {
					reply = integer(1);
				}else{
					reply = integer(0);
				}
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}

	@Override
	public Reply incr(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				String num = cresult.getAttrMap().get(ClientConstants.ATOMIC_OPER_RESULT);
				reply = integer(Long.parseLong(num));
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}
	
	@Override
	public Reply incrby(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				String num = cresult.getAttrMap().get(ClientConstants.ATOMIC_OPER_RESULT);
				reply = integer(Long.parseLong(num));
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}
	
	
	@Override
	public Reply decr(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				String num = cresult.getAttrMap().get(ClientConstants.ATOMIC_OPER_RESULT);
				reply = integer(Long.parseLong(num));
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}
	
	@Override
	public Reply decrby(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				String num = cresult.getAttrMap().get(ClientConstants.ATOMIC_OPER_RESULT);
				reply = integer(Long.parseLong(num));
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}
	
	@Override
	public Reply del(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				String count = cresult.getAttrMap().get(ClientConstants.REMOVE_KEY_COUNT);
				reply = integer(Long.parseLong(count));
			}else{
				reply = new ErrorReply(cresult.getErrorCode());
			}
		}
		return reply;
	}
	
	@Override
	public Reply exists(CommandResult cresult) {
		Reply reply = null;
		if(cresult != null) {
			if(cresult.isSuccess()) {
				reply = integer(1);
			}else{
				reply = integer(0);
			}
		}
		return reply;
	}
	
	@Override
	public Reply ping(CommandResult cresult) {
		return PONG;
	}
	
	@Override
	public Reply echo(CommandResult cresult) {
		return new BulkReply(cresult.getData());
	}

}
