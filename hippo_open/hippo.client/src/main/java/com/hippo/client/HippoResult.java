package com.hippo.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.network.CommandResult;

/**
 * 
 * @author saitxuc
 * @param <T>
 *
 */
public class HippoResult extends CommandResult {

    private static final Logger LOG = LoggerFactory.getLogger(HippoResult.class);

    /**
     * 
     */
    private static final long serialVersionUID = -5188492828875161682L;

    public HippoResult(boolean isSuccess) {
        super(isSuccess);
    }

    public HippoResult(boolean isSuccess, Serializable message, int version) {
        super(isSuccess, message, version);
    }

    public HippoResult(boolean isSuccess, byte[] data, int version, long expireTime) {
        super(isSuccess, data, version, expireTime);
    }

    public HippoResult(boolean isSuccess, String errorCode, Serializable message) {
        super(isSuccess, errorCode, message);
    }

    public HippoResult(boolean isSuccess, byte[] data, int version) {
        super(isSuccess, data, version);
    }

    public java.io.Serializable getDataForObject(Serializer serializer, Class<Serializable> clazz) {
        try {
            Serializable object = serializer.deserialize(data, clazz);
            return object;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (InstantiationException e) {
            LOG.error(e.getMessage());
        } catch (IllegalAccessException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }
    
    public <T> T getDataForObject(Class<T> clazz) {
        try {
            KryoSerializer serializer = new KryoSerializer();
            Serializable object = serializer.deserialize(data, null);
            serializer.close();
            return (T) object;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (InstantiationException e) {
            LOG.error(e.getMessage());
        } catch (IllegalAccessException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

}
