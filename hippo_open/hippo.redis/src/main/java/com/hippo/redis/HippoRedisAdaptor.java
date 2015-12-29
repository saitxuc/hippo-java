package com.hippo.redis;

import com.hippo.client.ClientConstants;
import com.hippo.client.command.*;
import com.hippo.common.util.KeyUtil;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.EchoCommand;
import com.hippo.network.command.PingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.hippo.redis.util.Encoding.bytesToNum;
import static com.hippo.redis.util.Encoding.redisByteToBoolean;

/**
 * @author saitxuc
 */
public class HippoRedisAdaptor implements RedisAdaptor {

    protected static final Logger log = LoggerFactory.getLogger(HippoRedisAdaptor.class);

    private static final String DEFAULT_BUCK_NO = "0";
    private static final String DEFAULT_VERSION = "0";
    private final int defaultVal = 32 * 1024;
    private byte separator = CommandConstants.DEFAULT_BIT_OP_SEPRATOR;

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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        byte[] newKey = KeyUtil.getByteAccordingOffset(key0, offsetInt, separator, defaultVal);
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
        byte[] newKey = KeyUtil.getByteAccordingOffset(key0, offsetInt, separator, defaultVal);
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
}
