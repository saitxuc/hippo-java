package com.hippo.common.hessian.io;

import java.io.IOException;
import java.util.HashMap;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:15
 *
 */
public class ClassDeserializer extends AbstractMapDeserializer {
	private static final HashMap<String, Class> _primClasses = new HashMap<String, Class>();

	private ClassLoader _loader;

	public ClassDeserializer(ClassLoader loader) {
		_loader = loader;
	}

	public Class getType() {
		return Class.class;
	}

	public Object readMap(AbstractHessianInput in) throws IOException {
		int ref = in.addRef(null);

		String name = null;

		while (!in.isEnd()) {
			String key = in.readString();

			if (key.equals("name"))
				name = in.readString();
			else
				in.readObject();
		}

		in.readMapEnd();

		Object value = create(name);

		in.setRef(ref, value);

		return value;
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		int ref = in.addRef(null);

		String name = null;

		for (int i = 0; i < fieldNames.length; i++) {
			if ("name".equals(fieldNames[i]))
				name = in.readString();
			else
				in.readObject();
		}

		Object value = create(name);

		in.setRef(ref, value);

		return value;
	}

	Object create(String name) throws IOException {
		if (name == null)
			throw new IOException("Serialized Class expects name.");

		Class cl = _primClasses.get(name);

		if (cl != null)
			return cl;

		try {
			if (_loader != null)
				return Class.forName(name, false, _loader);
			else
				return Class.forName(name);
		} catch (Exception e) {
			throw new IOExceptionWrapper(e);
		}
	}

	static {
		_primClasses.put("void", void.class);
		_primClasses.put("boolean", boolean.class);
		_primClasses.put("java.lang.Boolean", Boolean.class);
		_primClasses.put("byte", byte.class);
		_primClasses.put("java.lang.Byte", Byte.class);
		_primClasses.put("char", char.class);
		_primClasses.put("java.lang.Character", Character.class);
		_primClasses.put("short", short.class);
		_primClasses.put("java.lang.Short", Short.class);
		_primClasses.put("int", int.class);
		_primClasses.put("java.lang.Integer", Integer.class);
		_primClasses.put("long", long.class);
		_primClasses.put("java.lang.Long", Long.class);
		_primClasses.put("float", float.class);
		_primClasses.put("java.lang.Float", Float.class);
		_primClasses.put("double", double.class);
		_primClasses.put("java.lang.Double", Double.class);
		_primClasses.put("java.lang.String", String.class);
	}
}
