package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:45
 *
 */
public abstract class AbstractDeserializer implements Deserializer {
	public Class getType() {
		return Object.class;
	}

	public Object readObject(AbstractHessianInput in) throws IOException {
		Object obj = in.readObject();

		String className = getClass().getName();

		if (obj != null)
			throw error(className + ": unexpected object "
					+ obj.getClass().getName() + " (" + obj + ")");
		else
			throw error(className + ": unexpected null value");
	}

	public Object readList(AbstractHessianInput in, int length)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	public Object readLengthList(AbstractHessianInput in, int length)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	public Object readMap(AbstractHessianInput in) throws IOException {
		Object obj = in.readObject();

		String className = getClass().getName();

		if (obj != null)
			throw error(className + ": unexpected object "
					+ obj.getClass().getName() + " (" + obj + ")");
		else
			throw error(className + ": unexpected null value");
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	protected HessianProtocolException error(String msg) {
		return new HessianProtocolException(msg);
	}

	protected String codeName(int ch) {
		if (ch < 0)
			return "end of file";
		else
			return "0x" + Integer.toHexString(ch & 0xff);
	}
}
