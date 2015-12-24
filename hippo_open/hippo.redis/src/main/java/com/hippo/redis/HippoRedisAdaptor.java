package com.hippo.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.client.ClientConstants;
import com.hippo.client.command.AtomicntCommand;
import com.hippo.client.command.ExistsCommand;
import com.hippo.client.command.GetBitCommand;
import com.hippo.client.command.GetCommand;
import com.hippo.client.command.RemoveCommand;
import com.hippo.client.command.RemoveListCommand;
import com.hippo.client.command.SetBitCommand;
import com.hippo.client.command.SetCommand;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.common.util.Logarithm;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.EchoCommand;
import com.hippo.network.command.PingCommand;

import static com.hippo.redis.util.Encoding.bytesToNum;
import static com.hippo.redis.util.Encoding.redisByteToBoolean;
/**
 * 
 * @author saitxuc
 *
 */
public class HippoRedisAdaptor implements RedisAdaptor {
	
	protected static final Logger log = LoggerFactory.getLogger(HippoRedisAdaptor.class);
	
	private static final String DEFAULT_BUCK_NO = "0";
	private static final String DEFAULT_VERSION = "0";
	private final int defaultVal = 32 * 1024;
	private byte[] separator = null;
	
	public HippoRedisAdaptor() {
		Serializer serializer = null;
		try {
        	serializer = new KryoSerializer();
        	separator = serializer.serialize(CommandConstants.BITSET_SEPRATOR);
        } catch (IOException e) {
            log.error("separator BITSET_SEPRATOR error!!", e);
        }finally {
        	if(serializer != null) {
        		serializer.close();
        	}
        }
	}
	
	@Override
	public Command set(byte[] key0, byte[] value1) {
		SetCommand command = new SetCommand();
		try { 
			fillHeader(command);
			byte[] databytes = new byte[key0.length + value1.length];
	        System.arraycopy(key0, 0, databytes, 0, key0.length);
	        System.arraycopy(value1, 0, databytes, key0.length, value1.length);
	        command.setKlength(key0.length);
	        command.setData(databytes);
	        command.setExpire(-1);
		}catch (Exception e) {
            log.error("Redis set command convert hippo command happened error! ", e);
            return null;
        } 
		
		return command;
	}

	@Override
	public Command setex(byte[] key0, byte[] seconds1, byte[] value2) {
		SetCommand command = new SetCommand();
		try { 
			fillHeader(command);
			Long expireL = new Long(bytesToNum(seconds1));
			int expire = expireL.intValue();
			byte[] databytes = new byte[key0.length + value2.length];
	        System.arraycopy(key0, 0, databytes, 0, key0.length);
	        System.arraycopy(value2, 0, databytes, key0.length, value2.length);
	        command.setKlength(key0.length);
	        command.setData(databytes);
	        command.setExpire(expire);
		}catch (Exception e) {
            log.error("Redis set command convert hippo command happened error! ", e);
            return null;
        } 
		
		return command;
	}

	@Override
	public Command get(byte[] key0) {
		GetCommand command = new GetCommand();
		fillHeader(command);
		command.setData(key0); 
		return command;
	}
	
	private void fillHeader(Command command) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(ClientConstants.HEAD_BUCKET_NO, DEFAULT_BUCK_NO);
		headers.put(ClientConstants.HEAD_VERSION, DEFAULT_VERSION);
		command.setHeaders(headers);
	}

	@Override
	public Command setbit(byte[] key0, byte[] offset1, byte[] value2) {
		Long offsetL = new Long(bytesToNum(offset1));
		int offsetInt = offsetL.intValue();
		boolean val = redisByteToBoolean(value2);
		SetBitCommand command = new SetBitCommand();
		fillHeader(command);
		byte[] newKey = getByteAccordingOffset(key0, offsetInt);
        command.setData(newKey);
        command.setExpire(-1);
        command.putHeadValue(CommandConstants.BIT_OFFSET, offsetInt + "");
        command.putHeadValue(CommandConstants.BIT_VAL, val + "");
		return command;
	}
	
	@Override
	public Command getbit(byte[] key0, byte[] offset1) {
		Long offsetL = new Long(bytesToNum(offset1));
		int offsetInt = offsetL.intValue();
		GetBitCommand command = new GetBitCommand();
		fillHeader(command);
        byte[] newKey = getByteAccordingOffset(key0, offsetInt);
        command.setData(newKey);
        command.putHeadValue(CommandConstants.BIT_OFFSET, offsetInt + "");
        return command;
	}
		
	@Override
	public Command incr(byte[] key0) {
		AtomicntCommand command = new AtomicntCommand();
        command.setData(key0);
        command.setInitv(0);
        command.setDelta(1);
        command.setExpire(-1);
        command.putHeadValue("checkValEdge", true + "");
		return command;
	}
	
	@Override
	public Command incrby(byte[] key0, byte[] increment1) {
		AtomicntCommand command = new AtomicntCommand();
        command.setData(key0);
        command.setInitv(0);
        command.setDelta(bytesToNum(increment1));
        command.setExpire(-1);
        command.putHeadValue("checkValEdge", true + "");
		return command;
	}
	
	@Override
	public Command decr(byte[] key0) {
		AtomicntCommand command = new AtomicntCommand();
        command.setData(key0);
        command.setInitv(Long.MAX_VALUE);
        command.setDelta(-1);
        command.setExpire(-1);
        command.putHeadValue("checkValEdge", true + "");
		return command;
	}
	
	@Override
	public Command decrby(byte[] key0, byte[] decrement1) {
		AtomicntCommand command = new AtomicntCommand();
        command.setData(key0);
        command.setInitv(Long.MAX_VALUE);
        command.setDelta(-bytesToNum(decrement1));
        command.setExpire(-1);
        command.putHeadValue("checkValEdge", true + "");
		return command;
	}
	
	@Override
	public Command del(byte[][] key0) {
		RemoveListCommand removeListCommand = new RemoveListCommand();
		fillHeader(removeListCommand);
		for (byte[] bytes : key0) {
			RemoveCommand command = new RemoveCommand();
			fillHeader(command);
	        command.setData(bytes);
	        removeListCommand.addRemoveCommand(command);
		}
		return removeListCommand;
	}
	
	@Override
	public Command exists(byte[] key0) {
		ExistsCommand existsCommand = new ExistsCommand();
		fillHeader(existsCommand);
		existsCommand.setData(key0);
		return existsCommand;
	}
	
	@Override
	public Command ping() {
		PingCommand pcommand = new PingCommand(); 
		return pcommand;
	}
	
	@Override
	public Command echo(byte[] message0) {
		EchoCommand echoCommand = new EchoCommand();
		echoCommand.setData(message0);
		return echoCommand;
	}
	
    private byte[] getByteAccordingOffset(byte[] originalKey, int offset) {
        int byteSizeLeft = getByteSizeLeft(originalKey, defaultVal, separator);
        int blockOffset = offset / (byteSizeLeft * 8);
        byte[] offsetPerBlock = Logarithm.intToBytes((blockOffset + 1) * byteSizeLeft);
        return getKeyAfterCombineOffset(originalKey, offsetPerBlock, separator);
    }
	
    private int getByteSizeLeft(byte[] originalKey, int defaultVal, byte[] sep) {
        return defaultVal - 30 - originalKey.length - sep.length;
    }
    
    private byte[] getKeyAfterCombineOffset(byte[] originalKey, byte[] suffix, byte[] sep) {
        final byte[] newKey = new byte[originalKey.length + suffix.length + sep.length];
        System.arraycopy(originalKey, 0, newKey, 0, originalKey.length);
        System.arraycopy(sep, 0, newKey, originalKey.length - 1, sep.length);
        System.arraycopy(suffix, 0, newKey, originalKey.length - 1 + sep.length, suffix.length);
        return newKey;
    }


}
