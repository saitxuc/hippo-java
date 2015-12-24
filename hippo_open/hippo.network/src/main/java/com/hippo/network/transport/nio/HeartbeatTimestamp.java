package com.hippo.network.transport.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

/**
 * @author zhao.liangshu
 * @version 2014年8月6日 上午9:33:46
 */
public class HeartbeatTimestamp {
    
    private static final String HEARTBEAT_READ_KEY = "heartbeat-timestamp-read";
    
    private static final String HEARTBEAT_WRITE_KEY = "heartbeat-timestamp-write";
    
    private static final AttributeKey<Long> NET_HEARTBEAT_READ_KEY = AttributeKey.valueOf(HEARTBEAT_READ_KEY);
    
    private static final AttributeKey<Long> NET_HEARTBEAT_WRITE_KEY = AttributeKey.valueOf(HEARTBEAT_WRITE_KEY);
    
    public static void setHeartbeatReadTimestamp(ChannelHandlerContext ctx) {
        if (ctx != null) {
            ctx.channel().attr(NET_HEARTBEAT_READ_KEY).set(System.nanoTime());
        }
    }
    
    public static Long getHeartbeatReadTimestamp(ChannelHandlerContext ctx) {
        if (ctx != null) {
            return ctx.channel().attr(NET_HEARTBEAT_READ_KEY).get();
        }
        return 0l;
    }
    
    public static void setHeartbeatWriteTimestamp(ChannelHandlerContext ctx) {
        if (ctx != null) {
            ctx.channel().attr(NET_HEARTBEAT_WRITE_KEY).set(System.nanoTime());
        }
    }
    
    public static void setHeartbeatWriteTimestamp(Channel channel) {
        if (channel != null) {
            channel.attr(NET_HEARTBEAT_WRITE_KEY).set(System.nanoTime());
        }
    }
    
    public static Long getHeartbeatWriteTimestamp(ChannelHandlerContext ctx) {
        if (ctx != null) {
            return ctx.channel().attr(NET_HEARTBEAT_WRITE_KEY).get();
        }
        return 0l;
    }

}
