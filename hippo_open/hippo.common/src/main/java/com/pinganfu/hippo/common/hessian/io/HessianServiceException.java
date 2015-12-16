package com.pinganfu.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:10
 *
 */
public class HessianServiceException extends Exception {
	private String code;
	private Object detail;

	/**
	 * Zero-arg constructor.
	 */
	public HessianServiceException() {
	}

	/**
	 * Create the exception.
	 */
	public HessianServiceException(String message, String code, Object detail) {
		super(message);
		this.code = code;
		this.detail = detail;
	}

	/**
	 * Returns the code.
	 */
	public String getCode() {
		return code;
	}

	/**
	 * Returns the detail.
	 */
	public Object getDetail() {
		return detail;
	}
}
