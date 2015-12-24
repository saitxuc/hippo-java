package com.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14;25
 *
 */
public class HessianFieldException extends HessianProtocolException {
	/**
	 * Zero-arg constructor.
	 */
	public HessianFieldException() {
	}

	/**
	 * Create the exception.
	 */
	public HessianFieldException(String message) {
		super(message);
	}

	/**
	 * Create the exception.
	 */
	public HessianFieldException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Create the exception.
	 */
	public HessianFieldException(Throwable cause) {
		super(cause);
	}
}
