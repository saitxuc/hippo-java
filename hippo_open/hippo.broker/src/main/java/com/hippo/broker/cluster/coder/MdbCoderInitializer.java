package com.hippo.broker.cluster.coder;

import io.netty.handler.codec.ByteToMessageDecoder;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.transport.nio.coder.DefaultCoderInitializer;
import com.hippo.network.transport.nio.coder.DefaultSerializerDecoder;

/**
 * 
 * @author saitxuc
 *
 */
public class MdbCoderInitializer extends DefaultCoderInitializer {
    
    public MdbCoderInitializer() {
        super();
    }

    public MdbCoderInitializer(Serializer serializer) {
    	super(serializer);
    }

    @Override
    public ByteToMessageDecoder getDecoder() {
        return new DefaultSerializerDecoder(1048576 * 2, serializer);
    }


}

