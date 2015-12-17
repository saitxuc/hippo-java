package com.hippo.network.transport;

/**
 * 
 * @author saitxuc
 *
 */
public class ExceededMaximumConnectionsException extends Exception {

    /**
     * Default serial version id for serialization
     */
    private static final long serialVersionUID = -1166885550766355524L;

    public ExceededMaximumConnectionsException(String message) {
        super(message);
    }

}
