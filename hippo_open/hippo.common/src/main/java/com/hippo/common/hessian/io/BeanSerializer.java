package com.hippo.common.hessian.io;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.*;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:16
 *
 */
public class BeanSerializer extends AbstractSerializer {
	private static final Logger log = Logger.getLogger(BeanSerializer.class
			.getName());

	private static final Object[] NULL_ARGS = new Object[0];
	private Method[] _methods;
	private String[] _names;

	private Object _writeReplaceFactory;
	private Method _writeReplace;

	public BeanSerializer(Class cl, ClassLoader loader) {
		introspectWriteReplace(cl, loader);

		ArrayList primitiveMethods = new ArrayList();
		ArrayList compoundMethods = new ArrayList();

		for (; cl != null; cl = cl.getSuperclass()) {
			Method[] methods = cl.getDeclaredMethods();

			for (int i = 0; i < methods.length; i++) {
				Method method = methods[i];

				if (Modifier.isStatic(method.getModifiers()))
					continue;

				if (method.getParameterTypes().length != 0)
					continue;

				String name = method.getName();

				if (!name.startsWith("get"))
					continue;

				Class type = method.getReturnType();

				if (type.equals(void.class))
					continue;

				if (findSetter(methods, name, type) == null)
					continue;

				// XXX: could parameterize the handler to only deal with public
				method.setAccessible(true);

				if (type.isPrimitive()
						|| type.getName().startsWith("java.lang.")
						&& !type.equals(Object.class))
					primitiveMethods.add(method);
				else
					compoundMethods.add(method);
			}
		}

		ArrayList methodList = new ArrayList();
		methodList.addAll(primitiveMethods);
		methodList.addAll(compoundMethods);

		Collections.sort(methodList, new MethodNameCmp());

		_methods = new Method[methodList.size()];
		methodList.toArray(_methods);

		_names = new String[_methods.length];

		for (int i = 0; i < _methods.length; i++) {
			String name = _methods[i].getName();

			name = name.substring(3);

			int j = 0;
			for (; j < name.length() && Character.isUpperCase(name.charAt(j)); j++) {
			}

			if (j == 1)
				name = name.substring(0, j).toLowerCase() + name.substring(j);
			else if (j > 1)
				name = name.substring(0, j - 1).toLowerCase()
						+ name.substring(j - 1);

			_names[i] = name;
		}
	}

	private void introspectWriteReplace(Class cl, ClassLoader loader) {
		try {
			String className = cl.getName() + "HessianSerializer";

			Class serializerClass = Class.forName(className, false, loader);

			Object serializerObject = serializerClass.newInstance();

			Method writeReplace = getWriteReplace(serializerClass, cl);

			if (writeReplace != null) {
				_writeReplaceFactory = serializerObject;
				_writeReplace = writeReplace;

				return;
			}
		} catch (ClassNotFoundException e) {
		} catch (Exception e) {
			log.log(Level.FINER, e.toString(), e);
		}

		_writeReplace = getWriteReplace(cl);
	}

	/**
	 * Returns the writeReplace method
	 */
	protected Method getWriteReplace(Class cl) {
		for (; cl != null; cl = cl.getSuperclass()) {
			Method[] methods = cl.getDeclaredMethods();

			for (int i = 0; i < methods.length; i++) {
				Method method = methods[i];

				if (method.getName().equals("writeReplace")
						&& method.getParameterTypes().length == 0)
					return method;
			}
		}

		return null;
	}

	/**
	 * Returns the writeReplace method
	 */
	protected Method getWriteReplace(Class cl, Class param) {
		for (; cl != null; cl = cl.getSuperclass()) {
			for (Method method : cl.getDeclaredMethods()) {
				if (method.getName().equals("writeReplace")
						&& method.getParameterTypes().length == 1
						&& param.equals(method.getParameterTypes()[0]))
					return method;
			}
		}

		return null;
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (out.addRef(obj))
			return;

		Class cl = obj.getClass();

		try {
			if (_writeReplace != null) {
				Object repl;

				if (_writeReplaceFactory != null)
					repl = _writeReplace.invoke(_writeReplaceFactory, obj);
				else
					repl = _writeReplace.invoke(obj);

				out.removeRef(obj);

				out.writeObject(repl);

				out.replaceRef(repl, obj);

				return;
			}
		} catch (Exception e) {
			log.log(Level.FINER, e.toString(), e);
		}

		int ref = out.writeObjectBegin(cl.getName());

		if (ref < -1) {
			// Hessian 1.1 uses a map

			for (int i = 0; i < _methods.length; i++) {
				Method method = _methods[i];
				Object value = null;

				try {
					value = _methods[i].invoke(obj, (Object[]) null);
				} catch (Exception e) {
					log.log(Level.FINE, e.toString(), e);
				}

				out.writeString(_names[i]);

				out.writeObject(value);
			}

			out.writeMapEnd();
		} else {
			if (ref == -1) {
				out.writeInt(_names.length);

				for (int i = 0; i < _names.length; i++)
					out.writeString(_names[i]);

				out.writeObjectBegin(cl.getName());
			}

			for (int i = 0; i < _methods.length; i++) {
				Method method = _methods[i];
				Object value = null;

				try {
					value = _methods[i].invoke(obj, (Object[]) null);
				} catch (Exception e) {
					log.log(Level.FINER, e.toString(), e);
				}

				out.writeObject(value);
			}
		}
	}

	/**
	 * Finds any matching setter.
	 */
	private Method findSetter(Method[] methods, String getterName, Class arg) {
		String setterName = "set" + getterName.substring(3);

		for (int i = 0; i < methods.length; i++) {
			Method method = methods[i];

			if (!method.getName().equals(setterName))
				continue;

			if (!method.getReturnType().equals(void.class))
				continue;

			Class[] params = method.getParameterTypes();

			if (params.length == 1 && params[0].equals(arg))
				return method;
		}

		return null;
	}

	static class MethodNameCmp implements Comparator<Method> {
		public int compare(Method a, Method b) {
			return a.getName().compareTo(b.getName());
		}
	}
}
