package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:28
 *
 */
public class StringValueDeserializer extends AbstractDeserializer {
	private Class _cl;
	private Constructor _constructor;

	public StringValueDeserializer(Class cl) {
		try {
			_cl = cl;
			_constructor = cl.getConstructor(new Class[] { String.class });
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Class getType() {
		return _cl;
	}

	public Object readMap(AbstractHessianInput in) throws IOException {
		String value = null;

		while (!in.isEnd()) {
			String key = in.readString();

			if (key.equals("value"))
				value = in.readString();
			else
				in.readObject();
		}

		in.readMapEnd();

		Object object = create(value);

		in.addRef(object);

		return object;
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		String value = null;

		for (int i = 0; i < fieldNames.length; i++) {
			if ("value".equals(fieldNames[i]))
				value = in.readString();
			else
				in.readObject();
		}

		Object object = create(value);

		in.addRef(object);

		return object;
	}

	private Object create(String value) throws IOException {
		if (value == null)
			throw new IOException(_cl.getName() + " expects name.");

		try {
			return _constructor.newInstance(new Object[] { value });
		} catch (Exception e) {
			throw new IOExceptionWrapper(e);
		}
	}
}
