package com.pinganfu.hippo.network.transport.netty;

import java.io.IOException;
import java.net.URI;

import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.TransportFactory;
import com.pinganfu.hippo.network.transport.Transport;
import com.pinganfu.hippo.network.transport.TransportServer;

/**
 * DO NOT USE THIS CLASS, IT'S A TRASH BUT NEEDED BECAUSE OWL INCLUDED THIS SPI BY MISTAKE
 * @author DPJ
 * @deprecated
 */
public class NettyTransportFactory implements TransportFactory {

    /** 
     * @see com.pinganfu.hippo.network.TransportFactory#connect(java.net.URI, com.pinganfu.hippo.network.CommandManager)
     * @deprecated
     */
    @Override
    public Transport connect(URI uri, CommandManager commandManager) throws Exception {
        return null;
    }

    /** 
     * @see com.pinganfu.hippo.network.TransportFactory#connect(java.lang.String, int, com.pinganfu.hippo.network.CommandManager)
     * @deprecated
     */
    @Override
    public Transport connect(String host, int port, CommandManager commandManager) throws Exception {
        return null;
    }

    /** 
     * @see com.pinganfu.hippo.network.TransportFactory#bind(int, com.pinganfu.hippo.network.CommandManager)
     * @deprecated
     */
    @Override
    public TransportServer bind(int port, CommandManager commandManager) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /** 
     * @see com.pinganfu.hippo.network.TransportFactory#getName()
     * @deprecated
     */
    @Override
    public String getName() {
        return null;
    }

}
