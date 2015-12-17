package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:20
 *
 */
public class HessianProtocolException extends IOException {
	private Throwable rootCause;

	/**
	 * Zero-arg constructor.
	 */
	public HessianProtocolException() {
	}

	/**
	 * Create the exception.
	 */
	public HessianProtocolException(String message) {
		super(message);
	}

	/**
	 * Create the exception.
	 */
	public HessianProtocolException(String message, Throwable rootCause) {
		super(message);

		this.rootCause = rootCause;
	}

	/**
	 * Create the exception.
	 */
	public HessianProtocolException(Throwable rootCause) {
		super(String.valueOf(rootCause));

		this.rootCause = rootCause;
	}

	/**
	 * Returns the underlying cause.
	 */
	public Throwable getRootCause() {
		return rootCause;
	}

	/**
	 * Returns the underlying cause.
	 */
	public Throwable getCause() {
		return getRootCause();
	}
}
