package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time; 14:50
 *
 */
public class EnumSerializer extends AbstractSerializer {
	private Method _name;

	public EnumSerializer(Class cl) {
		// hessian/32b[12], hessian/3ab[23]
		if (!cl.isEnum() && cl.getSuperclass().isEnum())
			cl = cl.getSuperclass();

		try {
			_name = cl.getMethod("name", new Class[0]);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (out.addRef(obj))
			return;

		Class cl = obj.getClass();

		if (!cl.isEnum() && cl.getSuperclass().isEnum())
			cl = cl.getSuperclass();

		String name = null;
		try {
			name = (String) _name.invoke(obj, (Object[]) null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		int ref = out.writeObjectBegin(cl.getName());

		if (ref < -1) {
			out.writeString("name");
			out.writeString(name);
			out.writeMapEnd();
		} else {
			if (ref == -1) {
				out.writeClassFieldLength(1);
				out.writeString("name");
				out.writeObjectBegin(cl.getName());
			}

			out.writeString(name);
		}
	}
}
