package com.hippo.common.hessian.io;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:33
 *
 */
public class SqlDateDeserializer extends AbstractDeserializer {
	private Class _cl;
	private Constructor _constructor;

	public SqlDateDeserializer(Class cl) throws NoSuchMethodException {
		_cl = cl;
		_constructor = cl.getConstructor(new Class[] { long.class });
	}

	public Class getType() {
		return _cl;
	}

	public Object readMap(AbstractHessianInput in) throws IOException {
		int ref = in.addRef(null);

		long initValue = Long.MIN_VALUE;

		while (!in.isEnd()) {
			String key = in.readString();

			if (key.equals("value"))
				initValue = in.readUTCDate();
			else
				in.readString();
		}

		in.readMapEnd();

		Object value = create(initValue);

		in.setRef(ref, value);

		return value;
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		int ref = in.addRef(null);

		long initValue = Long.MIN_VALUE;

		for (int i = 0; i < fieldNames.length; i++) {
			String key = fieldNames[i];

			if (key.equals("value"))
				initValue = in.readUTCDate();
			else
				in.readObject();
		}

		Object value = create(initValue);

		in.setRef(ref, value);

		return value;
	}

	private Object create(long initValue) throws IOException {
		if (initValue == Long.MIN_VALUE)
			throw new IOException(_cl.getName() + " expects name.");

		try {
			return _constructor
					.newInstance(new Object[] { new Long(initValue) });
		} catch (Exception e) {
			throw new IOExceptionWrapper(e);
		}
	}
}
