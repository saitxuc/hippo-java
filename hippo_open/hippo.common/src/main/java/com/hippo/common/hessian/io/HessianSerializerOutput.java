package com.hippo.common.hessian.io;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:15
 *
 */
public class HessianSerializerOutput extends HessianOutput {
	/**
	 * Creates a new Hessian output stream, initialized with an underlying
	 * output stream.
	 * 
	 * @param os
	 *            the underlying output stream.
	 */
	public HessianSerializerOutput(OutputStream os) {
		super(os);
	}

	/**
	 * Creates an uninitialized Hessian output stream.
	 */
	public HessianSerializerOutput() {
	}

	/**
	 * Applications which override this can do custom serialization.
	 * 
	 * @param object
	 *            the object to write.
	 */
	public void writeObjectImpl(Object obj) throws IOException {
		Class cl = obj.getClass();

		try {
			Method method = cl.getMethod("writeReplace", new Class[0]);
			Object repl = method.invoke(obj, new Object[0]);

			writeObject(repl);
			return;
		} catch (Exception e) {
		}

		try {
			writeMapBegin(cl.getName());
			for (; cl != null; cl = cl.getSuperclass()) {
				Field[] fields = cl.getDeclaredFields();
				for (int i = 0; i < fields.length; i++) {
					Field field = fields[i];

					if (Modifier.isTransient(field.getModifiers())
							|| Modifier.isStatic(field.getModifiers()))
						continue;

					// XXX: could parameterize the handler to only deal with
					// public
					field.setAccessible(true);

					writeString(field.getName());
					writeObject(field.get(obj));
				}
			}
			writeMapEnd();
		} catch (IllegalAccessException e) {
			throw new IOExceptionWrapper(e);
		}
	}
}
