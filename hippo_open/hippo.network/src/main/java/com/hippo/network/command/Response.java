package com.hippo.network.command;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public class Response extends Command {
	
	private static final String ERROR_CODE_KEY = "errorcode";
	
	/**xxxx
	 * 
	 */
	private static final long serialVersionUID = 7028244689957315946L;
	
	private boolean failure = false;
	
	public Response() {
		super();
	}

	public boolean isFailure() {
		return failure;
	}

	public void setFailure(boolean failure) {
		this.failure = failure;
	}
	
	public void setErrorCode(String errorCode) {
		this.headers.put(ERROR_CODE_KEY, errorCode);
	}
	
	public String getErrorCode() {
		return this.headers.get(ERROR_CODE_KEY);
	}
}
