package com.hippo.common.hessian;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 11:55 
 *
 */
public class HessianException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public HessianException() {
		
	}
	
	/**
	 * Create the exception.
	 */
	public HessianException(String message) {
		super(message);
	}

	/**
	 * Create the exception.
	 */
	public HessianException(String message, Throwable rootCause) {
		super(message, rootCause);
	}

	/**
	 * Create the exception.
	 */
	public HessianException(Throwable rootCause) {
		super(rootCause);
	}
	
}
