package com.hippo.common.util;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.hessian.io.Hessian2Input;
import com.hippo.common.hessian.io.Hessian2Output;
import com.hippo.common.hessian.io.SerializerFactory;


/**
 * 
 * @author sait.xuc 
 * Date: 13/9/25
 * Time: 15:10
 *
 */
public class HessianUtil {
	
	private static final Logger log = LoggerFactory.getLogger(HessianUtil.class);
	private static final SerializerFactory serializerFactory = new SerializerFactory();
	
    public static Serializable deserialize(byte[] array) throws IOException {
        Object obj = null;
        ByteArrayInputStream in = new ByteArrayInputStream(array);
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
        return (Serializable)obj;
    }

    public static byte[] serialize(Object data) throws java.io.IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        Hessian2Output h2o = new Hessian2Output(out);
        h2o.setSerializerFactory(serializerFactory);
        try {
            h2o.writeObject(data);
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
	
}
