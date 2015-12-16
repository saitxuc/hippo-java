package com.pinganfu.hippo.mdb.exception;

import com.pinganfu.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 * write 2014-7-30
 */
public class OutOfMaxCapacityException extends HippoException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5820052817392577882L;
	private String errorCode;

    public OutOfMaxCapacityException(String reason, String errorCode) {
        super(reason);
        this.errorCode = errorCode;
    }

    public OutOfMaxCapacityException(String reason) {
        this(reason, null);
    }

    public String getErrorCode() {
        return errorCode;
    }
	
}