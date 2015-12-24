package com.hippo.network.exception;

import java.io.IOException;

import com.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 * write 2014-7-14
 */
public class ConnectionFailedException extends HippoException {

    private static final long serialVersionUID = 2288453203492073973L;

    public ConnectionFailedException(IOException cause) {
        super("The Hippo connection has failed: " + extractMessage(cause));
        initCause(cause);
        setLinkedException(cause);
    }

    public ConnectionFailedException() {
        super("The Hippo connection has failed due to a Transport problem");
    }

    private static String extractMessage(IOException cause) {
        String m = cause.getMessage();
        if (m == null || m.length() == 0) {
            m = cause.toString();
        }
        return m;
    }
}
