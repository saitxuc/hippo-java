package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

/**
 * 
 * @author sait.xuc
 * date: 13/9/29
 * Time: 14:35
 *
 */
public class Hessian2StreamingOutput {
	private Hessian2Output _out;

	/**
	 * Creates a new Hessian output stream, initialized with an underlying
	 * output stream.
	 * 
	 * @param os
	 *            the underlying output stream.
	 */
	public Hessian2StreamingOutput(OutputStream os) {
		_out = new Hessian2Output(os);
	}

	public void setCloseStreamOnClose(boolean isClose) {
		_out.setCloseStreamOnClose(isClose);
	}

	public boolean isCloseStreamOnClose() {
		return _out.isCloseStreamOnClose();
	}

	/**
	 * Writes any object to the output stream.
	 */
	public void writeObject(Object object) throws IOException {
		_out.writeStreamingObject(object);
	}

	/**
	 * Flushes the output.
	 */
	public void flush() throws IOException {
		_out.flush();
	}

	/**
	 * Close the output.
	 */
	public void close() throws IOException {
		_out.close();
	}
}
