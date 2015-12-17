package com.hippo.mdb.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.hessian.io.Hessian2Input;
import com.hippo.common.hessian.io.Hessian2Output;
import com.hippo.common.hessian.io.SerializerFactory;
import com.hippo.common.serializer.Serializer;



public class HessionSerializer implements Serializer {
	
	private static final Logger log = LoggerFactory.getLogger(HessionSerializer.class);
	
	private static final SerializerFactory serializerFactory = new SerializerFactory();
	
	@Override
	public <T> byte[] serialize(T obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        Hessian2Output h2o = new Hessian2Output(out);
        h2o.setSerializerFactory(serializerFactory);
        try {
            h2o.writeObject(obj);
        } finally {
            try {
                h2o.close();
                out.close();
            } catch (IOException ex) {
                log.error("Failed to close stream.", ex);
            }
        }
        return out.toByteArray();
	}

	@Override
	public <T> T deserialize(byte[] source, Class<T> clazz) throws IOException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		Object obj = null;
        ByteArrayInputStream in = new ByteArrayInputStream(source);
        Hessian2Input hin = new Hessian2Input(in);
        hin.setSerializerFactory(serializerFactory);
        try {
            obj = hin.readObject();
        } finally {
            try {
                hin.close();
                in.close();
            } catch (IOException ex) {
                log.error("Failed to close stream.", ex);
            }
        }
        return clazz.cast(obj);
	}

	@Override
	public String getName() {
		return "hession";
	}

	@Override
	public void close() {
		
	}

}
