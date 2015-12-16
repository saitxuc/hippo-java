package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc Date: 13/9/29 Time: 15:25
 * 
 */
public class ArraySerializer extends AbstractSerializer {
	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (out.addRef(obj))
			return;

		Object[] array = (Object[]) obj;

		boolean hasEnd = out.writeListBegin(array.length,
				getArrayType(obj.getClass()));

		for (int i = 0; i < array.length; i++)
			out.writeObject(array[i]);

		if (hasEnd)
			out.writeListEnd();
	}

	/**
	 * Returns the &lt;type> name for a &lt;list>.
	 */
	private String getArrayType(Class cl) {
		if (cl.isArray())
			return '[' + getArrayType(cl.getComponentType());

		String name = cl.getName();

		if (name.equals("java.lang.String"))
			return "string";
		else if (name.equals("java.lang.Object"))
			return "object";
		else if (name.equals("java.util.Date"))
			return "date";
		else
			return name;
	}
}
