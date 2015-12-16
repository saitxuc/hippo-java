package com.pinganfu.hippo.common.exception;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public class HippoException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7207483076543843436L;

	private String errorCode;

    private Exception linkedException;
    
    public HippoException(String reason, String errorCode, Exception linkedException) {
        super(reason);
        this.errorCode = errorCode;
        linkedException = linkedException;
    }
    
    public HippoException(String reason, String errorCode) {
        super(reason);
        this.errorCode = errorCode;
        linkedException = null;
    }

    public HippoException(String reason) {
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
