package com.hippo.network.exception;

import java.io.IOException;

/**
 * 
 * @author saitxuc
 * write 2014-7-14
 */
public class ConnectionClosedException extends IllegalStateException {
    private static final long serialVersionUID = -7681404582227153308L;

    public ConnectionClosedException() {
        super("The connection is already closed, Hippo connect close.");
    }
    
    
}