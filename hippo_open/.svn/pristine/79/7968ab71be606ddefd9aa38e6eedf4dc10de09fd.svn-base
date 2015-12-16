package com.pinganfu.hippo.common.exception;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public class ConfigurationException extends HippoException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5820052817392577882L;
	private String errorCode;

    public ConfigurationException(String reason, String errorCode) {
        super(reason);
        this.errorCode = errorCode;
    }

    public ConfigurationException(String reason) {
        this(reason, null);
    }

    public String getErrorCode() {
        return errorCode;
    }
	
}
