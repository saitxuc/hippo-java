package com.pinganfu.hippo.broker.cluster;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.ConnectionCallBack;
import com.pinganfu.hippo.network.command.ConnectionInfo;
import com.pinganfu.hippo.network.transport.nio.server.NioTransportConnectionManager;

/**
 * @author yangxin
 */
public class ReplicationNioConnectionManager extends NioTransportConnectionManager {
    private final static Logger log = LoggerFactory.getLogger(ReplicationNioConnectionManager.class);

    List<ConnectionCallBack> callbacks = null;
    /**
     * key:ip, value:channel
     */
    private Map<String, Channel> channelMap = new ConcurrentHashMap<String, Channel>();

    @Override
    public synchronized void addConnectionInfo(Object key, ConnectionInfo info) throws Exception {
        super.addConnectionInfo(key, info);

        ChannelHandlerContext ctx = (ChannelHandlerContext) key;
        Channel channel = ctx.channel();
        String clientId = info.getClientId();
        if (log.isInfoEnabled()) {
            log.info("Add new connection[clientId:{}, slaveIp:{}] to channelMap.", clientId, channel.remoteAddress().toString());
        }
        channelMap.put(clientId, channel);
    }

    public synchronized Channel lease(String standby) throws IOException {
        Channel channel = channelMap.get(standby);
        if (channel != null) {
            if (channel.isActive()) {
                return channel;
            } else {
                log.error("channel[{}] has disconnect.", channel.remoteAddress().toString());
                log.info("detect the slave -> " + standby + " not active, it will be removed");
                channelMap.remove(standby);
                if (callbacks != null) {
                    for (ConnectionCallBack callback : callbacks) {
                        callback.removeConnectionCallBack(standby);
                    }
                }
            }
        }
        throw new IOException("Slave[" + standby + "] has disconnect!");
    }

    public List<ConnectionCallBack> getCallbacks() {
        return callbacks;
    }

    public void setCallbacks(List<ConnectionCallBack> callbacks) {
        this.callbacks = callbacks;
    }

}
