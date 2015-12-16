package com.pinganfu.hippo.network.transport.nio.coder;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import com.pinganfu.hippo.common.serializer.KryoSerializer;
import com.pinganfu.hippo.common.serializer.Serializer;
import com.pinganfu.hippo.network.command.Command;

public class MdbCoderInitializer extends DefaultCoderInitializer {
    private Serializer serializer = null;

    public MdbCoderInitializer() {
        this.serializer = new KryoSerializer();
    }

    public MdbCoderInitializer(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public ByteToMessageDecoder getDecoder() {
        return new DefaultSerializerDecoder(1048576 * 2, serializer);
    }

    @Override
    public MessageToByteEncoder<Command> getEncoder() {
        return super.getEncoder();
    }
}
