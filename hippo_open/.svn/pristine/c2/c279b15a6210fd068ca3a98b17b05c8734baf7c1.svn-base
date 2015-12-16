package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:59
 *
 */
public class IOExceptionWrapper extends IOException {
	private Throwable _cause;

	public IOExceptionWrapper(Throwable cause) {
		super(cause.toString());

		_cause = cause;
	}

	public IOExceptionWrapper(String msg, Throwable cause) {
		super(msg);

		_cause = cause;
	}

	public Throwable getCause() {
		return _cause;
	}
}
