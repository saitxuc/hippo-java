package com.hippo.store.exception;

/**
 * 
 * @author saitxuc
 * write 2014-7-22
 */
public class HippoStoreException extends Exception{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4440814176738060875L;

	private String errorCode;

    private Exception linkedException;

    public HippoStoreException(String reason, String errorCode) {
        super(reason);
        this.errorCode = errorCode;
        linkedException = null;
    }

    public HippoStoreException(String reason) {
        this(reason, null);
        linkedException = null;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public Exception getLinkedException() {
        return linkedException;
    }

    public synchronized void setLinkedException(Exception ex) {
        linkedException = ex;
    }
}
