package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:39
 *
 */
public class ObjectDeserializer extends AbstractDeserializer {
	private Class _cl;

	public ObjectDeserializer(Class cl) {
		_cl = cl;
	}

	public Class getType() {
		return _cl;
	}

	public Object readObject(AbstractHessianInput in) throws IOException {
		return in.readObject();
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	public Object readList(AbstractHessianInput in, int length)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	public Object readLengthList(AbstractHessianInput in, int length)
			throws IOException {
		throw new UnsupportedOperationException(String.valueOf(this));
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + _cl + "]";
	}
}
