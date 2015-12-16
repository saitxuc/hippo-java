package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:02
 *
 */
public class EnumDeserializer extends AbstractDeserializer {
	private Class _enumType;
	private Method _valueOf;

	public EnumDeserializer(Class cl) {
		// hessian/33b[34], hessian/3bb[78]
		if (cl.isEnum())
			_enumType = cl;
		else if (cl.getSuperclass().isEnum())
			_enumType = cl.getSuperclass();
		else
			throw new RuntimeException("Class " + cl.getName()
					+ " is not an enum");

		try {
			_valueOf = _enumType.getMethod("valueOf", new Class[] {
					Class.class, String.class });
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Class getType() {
		return _enumType;
	}

	public Object readMap(AbstractHessianInput in) throws IOException {
		String name = null;

		while (!in.isEnd()) {
			String key = in.readString();

			if (key.equals("name"))
				name = in.readString();
			else
				in.readObject();
		}

		in.readMapEnd();

		Object obj = create(name);

		in.addRef(obj);

		return obj;
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		String name = null;

		for (int i = 0; i < fieldNames.length; i++) {
			if ("name".equals(fieldNames[i]))
				name = in.readString();
			else
				in.readObject();
		}

		Object obj = create(name);

		in.addRef(obj);

		return obj;
	}

	private Object create(String name) throws IOException {
		if (name == null)
			throw new IOException(_enumType.getName() + " expects name.");

		try {
			return _valueOf.invoke(null, _enumType, name);
		} catch (Exception e) {
			throw new IOExceptionWrapper(e);
		}
	}
}
